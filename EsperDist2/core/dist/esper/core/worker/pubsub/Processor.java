package dist.esper.core.worker.pubsub;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.*;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.pubsub.Subscriber.State;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.AbstractExpression;
import dist.esper.epl.expr.EventIndexedSpecification;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.EventPropertyIndexedSpecification;
import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.epl.expr.util.ExpressionStringlizer;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;
import dist.esper.event.EventRegistry;
import dist.esper.util.*;

public class Processor implements ISubscriberObserver{
	static Logger2 log;
	static Logger2 outputLog;
	static{
		log=Logger2.getLogger(Processor.class);
		outputLog=AsyncLogger2.getAsyncLogger(Processor.EPListener.class, Level.DEBUG, "log/output.txt", false, "%d{MM-dd HH:mm:ss} %p %m%n");		
	}
	
	String workerId;
	long id;
	State state=State.NONE;
	List<Subscriber> subList=new ArrayList<Subscriber>(2);
	List<IProcessorObserver> pubList=new ArrayList<IProcessorObserver>(2);
	EPListener epListener=new EPListener();	
	
	DerivedStreamContainer streamContainer=null;
	//EPAdministrator epAdmin=null;
	EPServiceProvider epService=null;
	EventRegistry eventRegistry=null;
	EPStatement epStatement=null;
	String epl;
	PublishingScheduler2 pubScheduler;
	InstanceStat instanceStat;
	boolean isLogQueryResult=false;
	static AtomicLong UID=new AtomicLong(0L);

	public Processor(String workerId, EPServiceProvider epService, 
			EventRegistry eventRegistry, 
			DerivedStreamContainer streamContainer,
			PublishingScheduler2 pubScheduler,
			InstanceStat instanceStat) {
		super();
		this.workerId = workerId;
		this.epService = epService;
		this.eventRegistry = eventRegistry;
		this.streamContainer = streamContainer;
		this.pubScheduler = pubScheduler;
		this.instanceStat = instanceStat;
		this.id=UID.getAndIncrement();
		String logQueryResultStr=ServiceManager.getConfig().get(Options.LOG_QUERY_RESULT);
		try{isLogQueryResult=Boolean.valueOf(logQueryResultStr);}catch(Exception ex){}
	}

	public void init(){
		if(streamContainer instanceof JoinDelayedStreamContainer){
			initJoinDelayed((JoinDelayedStreamContainer)streamContainer);
		}
		else if(streamContainer instanceof JoinStreamContainer){
			initJoin((JoinStreamContainer)streamContainer);
		}
		else if(streamContainer instanceof FilterDelayedStreamContainer){
			initFilterDelayed((FilterDelayedStreamContainer)streamContainer);
		}
		else if(streamContainer instanceof FilterStreamContainer){
			initFilter((FilterStreamContainer)streamContainer);
		}
		else if(streamContainer instanceof PatternStreamContainer){
			initPattern((PatternStreamContainer)streamContainer);
		}
		else if(streamContainer instanceof RootStreamContainer){
			initRoot((RootStreamContainer)streamContainer);
		}
		this.state=State.INITTED;
	}
	
	public void start(){
		StringBuilder sb=new StringBuilder();
		if(epStatement!=null && !epStatement.isDestroyed()){
			epStatement.destroy();
		}
		log.info("** start %s(id=%d)(eplId=%d)", this.streamContainer.getClass().getSimpleName(),
				this.streamContainer.getId(), this.streamContainer.getEplId());
		try{
			epStatement=epService.getEPAdministrator().createEPL(epl);
			epStatement.addListener(epListener);
			this.state=State.RUNNING;
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public void stop(){
		this.state=State.STOPPED;
		this.epStatement.stop();
	}
	
	public void resume(){
		start();
	}
	
	public void modify(DerivedStreamContainer newStreamContainer){
		assert(this.state==State.STOPPED);
		this.streamContainer=newStreamContainer;
		init();
	}
	
	
	public void initRoot(RootStreamContainer rsl){
		Event childEvent=rsl.getUpContainer().getInternalCompositeEvent();
		epService.getEPAdministrator().getConfiguration().addEventType(childEvent.getName(), childEvent);
		eventRegistry.registEvent(childEvent);
		log.debug("regist internal composite event: %s",childEvent.toString());
		
		//create EPL
		StringBuilder sw=new StringBuilder();
		sw.append("select ");
		String delimiter="";
		for(SelectClauseExpressionElement se: rsl.getResultElementList()){
			sw.append(delimiter);
			/*se.toStringBuilderWithNickName(sw);*/
			ExpressionNickNameStringlizer.getInstance().toStringBuilder(se, sw);
			sw.append(" as ");
			sw.append(se.getUniqueName());
			delimiter=", ";
		}
		
		sw.append(" from ");
		delimiter="";
		
		sw.append(rsl.getUpContainer().getInternalCompositeEvent().getName());//FIXME: ADD VIEW
		sw.append(" as ");
		sw.append(rsl.getUpContainer().getUniqueName());
		
		if(rsl.getWhereExprList().size()>0){
			sw.append(" where ");
			delimiter="";
			for(AbstractBooleanExpression joinExpr: rsl.getWhereExprList()){
				sw.append(delimiter);
				/*joinExpr.toStringBuilderWithNickName(sw);*/
				ExpressionNickNameStringlizer.getInstance().toStringBuilder(joinExpr, sw);
				delimiter=" and ";
			}
		}
		setEpl(sw.toString());
	}

	public void initPattern(PatternStreamContainer psl){
		for(RawStream child: psl.getRawStreamList()){
			Event childEvent=child.getInternalCompositeEvent();
			epService.getEPAdministrator().getConfiguration().addEventType(childEvent.getName(), childEvent);
			eventRegistry.registEvent(childEvent);
			log.debug("regist internal composite event: %s",childEvent.toString());
		}
		
		//create EPL
		StringBuilder sw=new StringBuilder();
		sw.append("select ");
		String delimiter="";
		for(SelectClauseExpressionElement se: psl.getResultElementList()){
			sw.append(delimiter);
			se.toStringBuilder(sw);
			sw.append(" as ");
			sw.append(se.getUniqueName());
			delimiter=", ";
		}
		
		sw.append(" from pattern[");
		psl.getPatternNode().toStringBuilder(sw);//FIXME: ADD VIEW
		sw.append("]");
		setEpl(sw.toString());
	}
	
	public void initFilterDelayed(FilterDelayedStreamContainer fcsl){
		Event agentEvent=fcsl.getAgent().getInternalCompositeEvent();
		epService.getEPAdministrator().getConfiguration().addEventType(agentEvent.getName(), agentEvent);
		eventRegistry.registEvent(agentEvent);
		log.debug("regist internal composite event: %s",agentEvent.toString());
		
		//create EPL
		StringBuilder sw=new StringBuilder();
		sw.append("select ");
		String delimiter="";
		for(SelectClauseExpressionElement se: fcsl.getResultElementList()){
			sw.append(delimiter);
			/*se.toStringBuilderWithNickName(sw);*/
			ExpressionNickNameStringlizer.getInstance().toStringBuilder(se, sw);
			sw.append(" as ");
			sw.append(se.getUniqueName());
			delimiter=", ";
		}
		
		sw.append(" from ");
		sw.append(agentEvent.getName());
		//FIXME
		/*AbstractBooleanExpression.toStringBuilderWithNickName(fcsl.getExtraFilterCondList(), RelationTypeEnum.AND, sw);*/
		ExpressionNickNameStringlizer.getInstance().toStringBuilder(fcsl.getExtraFilterCondList(), RelationTypeEnum.AND, sw);
		sw.append(" as ");
		sw.append(fcsl.getAgent().getUniqueName());//FIXME: ADD VIEW
		
		setEpl(sw.toString());
	}

	
	public void initFilter(FilterStreamContainer fsl){
		Event childEvent=fsl.getRawStream().getInternalCompositeEvent();
		epService.getEPAdministrator().getConfiguration().addEventType(childEvent.getName(), childEvent);
		eventRegistry.registEvent(childEvent);
		log.debug("regist internal composite event: %s",childEvent.toString());
		
		//create EPL
		StringBuilder sw=new StringBuilder();
		sw.append("select ");
		String delimiter="";
		for(SelectClauseExpressionElement se: fsl.getResultElementList()){
			sw.append(delimiter);
			se.toStringBuilder(sw);
			sw.append(" as ");
			sw.append(se.getUniqueName());
			delimiter=", ";
		}
		
		sw.append(" from ");
		sw.append(childEvent.getName());//FIXME: ADD VIEW

		//FIXME: fsl.getFilterExpr().toStringBuilder(sw);
		FilterExpressionStringlizer.getInstance().toStringBuilder(fsl.getFilterExpr(), sw);
		sw.append(" as ");
		fsl.getEventSpec().toStringBuilder(sw);
		setEpl(sw.toString());
	}
	
	public void initJoinDelayed(JoinDelayedStreamContainer jcsl){		
		Event agentEvent=jcsl.getAgent().getInternalCompositeEvent();
		epService.getEPAdministrator().getConfiguration().addEventType(agentEvent.getName(), agentEvent);
		eventRegistry.registEvent(agentEvent);
		log.debug("regist internal composite event: %s",agentEvent.toString());
		
		//create EPL
		StringBuilder sw=new StringBuilder();
		sw.append("select ");
		String delimiter="";
		for(SelectClauseExpressionElement se: jcsl.getResultElementList()){
			sw.append(delimiter);
			/*se.toStringBuilderWithNickName(sw);*/
			ExpressionNickNameStringlizer.getInstance().toStringBuilder(se, sw);
			sw.append(" as ");
			sw.append(se.getUniqueName());
			delimiter=", ";
		}
		
		sw.append(" from ");
		sw.append(agentEvent.getName());
		sw.append(" as ");
		sw.append(jcsl.getAgent().getUniqueName());//FIXME: ADD VIEW
		
		if(jcsl.getJoinExprList().size()>0){
			sw.append(" where ");
			if(jcsl.getExtraChildCondList().size()>0){
				/*AbstractBooleanExpression.toStringBuilderWithNickName(jcsl.getExtraChildCondList(), RelationTypeEnum.AND, sw);*/
				ExpressionNickNameStringlizer.getInstance().toStringBuilder(jcsl.getExtraChildCondList(), RelationTypeEnum.AND, sw);
			}
			if(jcsl.getExtraJoinCondList().size()>0){
				if(jcsl.getExtraChildCondList().size()>0){
					sw.append(" and ");
				}
				/*AbstractBooleanExpression.toStringBuilderWithNickName(jcsl.getExtraJoinCondList(), RelationTypeEnum.AND, sw);*/
				ExpressionNickNameStringlizer.getInstance().toStringBuilder(jcsl.getExtraJoinCondList(), RelationTypeEnum.AND, sw);
			}
		}
		setEpl(sw.toString());
	}

	public void initJoin(JoinStreamContainer jsl){
		for(StreamContainer child: jsl.getUpContainerList()){
			Event childEvent=child.getInternalCompositeEvent();
			epService.getEPAdministrator().getConfiguration().addEventType(childEvent.getName(), childEvent);
			eventRegistry.registEvent(childEvent);
			log.debug("regist internal composite event: %s",childEvent.toString());
		}
		
		//create EPL
		StringBuilder sw=new StringBuilder();
		sw.append("select ");
		String delimiter="";
		for(SelectClauseExpressionElement se: jsl.getResultElementList()){
			sw.append(delimiter);
			/*se.toStringBuilderWithNickName(sw);*/
			ExpressionNickNameStringlizer.getInstance().toStringBuilder(se, sw);//sl.uniqueName+"."+se.uniqueName
			sw.append(" as ");
			sw.append(se.getUniqueName());
			delimiter=", ";
		}
		
		sw.append(" from ");
		delimiter="";
		for(StreamContainer child: jsl.getUpContainerList()){
			sw.append(delimiter);
			sw.append(child.getInternalCompositeEvent().getName());//FIXME: ADD VIEW
			//sw.append(".");
			sw.append(getViewSpecsString(child));
			sw.append(" as ");
			sw.append(child.getUniqueName());
			delimiter=", ";
		}
		
		if(jsl.getJoinExprList().size()>0){
			sw.append(" where ");
			delimiter="";
			for(AbstractBooleanExpression joinExpr: jsl.getJoinExprList()){
				sw.append(delimiter);
				/*joinExpr.toStringBuilderWithNickName(sw);*/
				ExpressionNickNameStringlizer.getInstance().toStringBuilder(joinExpr, sw);
				delimiter=" and ";
			}
		}
		setEpl(sw.toString());
	}
	
	private String getViewSpecsString(StreamContainer sl){
		if(sl instanceof DerivedStreamContainer){
			DerivedStreamContainer sesl=(DerivedStreamContainer)sl;
//			if(sesl.getViewSpecs()!=null){
//				StringBuilder sb=new StringBuilder();			
//				String delimiter="";
//				for(ViewSpecification vs: sesl.getViewSpecs()){
//					sb.append(delimiter);
//					vs.toStringBuilder(sb);
//					delimiter=".";
//				}
//				return sb.toString();
//			}
			//return String.format("win:time(%d sec)", sesl.getWindowTimeUS()/1000);
			return sesl.getWindowTimeViewSpecString();
		}
		return ".win:time(10 sec)";
	}
	
	public void registerSubcriber(Subscriber sub){
		subList.add(sub);
		//sub.addObserver(this);
		sub.setObserver(this);
	}
	
	public void addObserver(IProcessorObserver pub){
		pubList.add(pub);
	}
	
	public InstanceStat getInstanceStat() {
		return instanceStat;
	}

	public void setInstanceStat(InstanceStat instanceStat) {
		this.instanceStat = instanceStat;
	}

	@Override
	public void updateSubscriberObserver(long subscriberId, String streamName, String[] elementNames, 
			String eventTypeName, Object[] events) {
		instanceStat.updateSubscriberStat(subscriberId, events.length);
		
		long startProcTimestampNS=System.nanoTime();
		if(events[0] instanceof Map<?,?>){			
			for(Object event: events){
				epService.getEPRuntime().sendEvent((Map<String,Object>)event, eventTypeName);
			}
		}
		else if(events[0] instanceof Object[]){
			try{
				if(this.streamContainer instanceof JoinDelayedStreamContainer){
					System.out.print("");
				}
				
				//might be RawStreamLocation or StreamLocationContainer
				Stream child=this.streamContainer.getUpStreamByEventName(eventTypeName);
				
				if(child==null || child.getInternalCompositeEvent()==null){
					System.out.print("");
				}
				//elementNames>=child.getCompositeEvent().getPropList();
				List<EventProperty> propList=child.getInternalCompositeEvent().getPropList();
				int[] propIndeces=new int[propList.size()];
				int count=0;
				for(int k=0; k<propList.size(); k++){					
					for(int j=0; j<elementNames.length; j++){
						if(propList.get(k).getName().equals(elementNames[j])){
							propIndeces[k]=j;
							count++;
							break;
						}
					}
				}
				if(count!=propList.size()){
					System.out.print("");
				}
				assert(count==propList.size());
				
				Map[] maps=new Map[events.length];
				for(int i=0;i<events.length;i++){
					Object[] attribs=(Object[])events[i];
					Map<String,Object> map=new TreeMap<String,Object>();
					for(int k=0; k<propList.size(); k++){//FIXME: the order is not the same
						map.put(propList.get(k).getName(), attribs[propIndeces[k]]);
					}
					maps[i]=map;
				}
				
				for(Map map: maps){
//					System.out.format("Processor[%s]: %s\n", streamLocationContainer.getClass().getSimpleName(), map.toString());
					epService.getEPRuntime().sendEvent(
							(Map<String,Object>)map, eventTypeName);
				}
				
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
		}
		long endProcTimestampNS=System.nanoTime();
		instanceStat.updateProcessorProcessingTime(
				(endProcTimestampNS-startProcTimestampNS)/1000.0,
				subscriberId,
				events.length);
	}
	
	public void logQueryResult(EventBean[] newEvents){		
		RootStreamContainer rsc=(RootStreamContainer)streamContainer;
		for(int i=0;i<rsc.getDirectReuseStreamMapComparisonResultList().size();i++){
			Stream stream=rsc.getDirectReuseStreamMapComparisonResultList().get(i).getFirst();
			for(EventBean event: newEvents){
				ResultElementData[] reds=new ResultElementData[stream.getResultElementList().size()];
				for(int j=0;j<stream.getResultElementList().size();j++){
					SelectClauseExpressionElement se=stream.getResultElementList().get(j);
					reds[j]=new ResultElementData(se.toString(), event.get(se.getUniqueName()));
				}
				outputLog.debug("eplId:%d, output: %s", stream.getEplId(), Arrays.asList(reds).toString());
			}
		}		
	}
	
	class ResultElementData extends Tuple2D<String,Object>{
		private static final long serialVersionUID = -7956967873667310298L;
		public ResultElementData(String first, Object second) {
			super(first, second);			
		}
		@Override
		public String toString(){
			return first+":"+second;
		}
	}
	
	
	class EPListener implements UpdateListener{
		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			if(newEvents==null){
				instanceStat.updateProcessorOutputCount(0);
				return;
			}
			instanceStat.updateProcessorOutputCount(newEvents.length);
			if(state!=State.RUNNING){
				System.err.format("** processor receives %s EventBeans in EPListener, but is not in RUNNING STATE.\n", newEvents.length);
				return;
			}
			if(isLogQueryResult && (streamContainer instanceof RootStreamContainer)){
				logQueryResult(newEvents);
			}
			for(IProcessorObserver pub: pubList){
				pubScheduler.sumbit(newEvents, pub);
			}
		}
	}
	
	public long getId() {
		return id;
	}	

	public String getEpl() {
		return epl;
	}

	public void setEpl(String epl) {
		this.epl = epl + String.format(" output every %d msec", 
				ServiceManager.getInstance(workerId).getOutputIntervalUS()/1000);
		log.info("Worker %s will start epl: %s", this.workerId, epl);
	}

	@Override
	public String toString(){
		return streamContainer.toString();
	}
	
	public enum State{
		NONE("none"),
		INITTED("inited"),		
		RUNNING("running"),
		PAUSE("pause"),
		STOPPED("stopped");
		
		String str;
		State(String str){
			this.str=str;
		}
		@Override
		public String toString(){
			return str;
		}
	}
}

