package combine.esper.core.worker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import com.espertech.esper.client.soda.FromClause;
import combine.esper.core.coordinator.Coordinator;

import dist.esper.core.comm.LinkManager;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.pubsub.ISubscriberObserver;
import dist.esper.core.worker.pubsub.ProcessingScheduler2;
import dist.esper.core.worker.pubsub.Subscriber;
import dist.esper.util.Logger2;

public class Instance implements ISubscriberObserver{
	static Logger2 log=Logger2.getLogger(Instance.class);
	String workerId;
	long id;
	LinkManager linkManager;
	EPServiceProvider epService;	
	EPStatement epStatement;
	List<RawStream> rawStreamList;
	ProcessingScheduler2 procScheduler;
	ISubscriberObserver proc=null;
	List<Subscriber> subList=new ArrayList<Subscriber>(2);
	EPListener epListener=new EPListener();	
	String epl;
	long eplId;
	
	static AtomicLong UID=new AtomicLong(0L);
	
	public Instance(String workerId, 
			LinkManager linkManager, EPServiceProvider epService,
			List<RawStream> rawStreamList,
			ProcessingScheduler2 procScheduler,			
			long eqlId, String epl){
		super();
		this.workerId = workerId;
		this.procScheduler = procScheduler;
		this.linkManager = linkManager;
		this.epService = epService;		
		this.rawStreamList = rawStreamList;
		this.eplId = eqlId;
		this.epl = epl;
		this.id=UID.getAndIncrement();
	}
	
	public void init() throws Exception{
		Collection<RawStream> rawStreams=parseRawStreams();
		for(RawStream rs: rawStreams){
			Subscriber sub=new Subscriber(
					workerId,
					rs.getUniqueName(),
					rs.getInternalCompositeEvent().getName(),
					rs.getResultElementUniqueNameList(),
					10*1000*1000, //ignore this argument
					rs.getWorkerId(),
					linkManager,
					procScheduler,
					null);
			sub.setObserver(this);
			this.subList.add(sub);
		}
		for(Subscriber sub: subList){
			sub.init();
		}
	}
	
	public Collection<RawStream> parseRawStreams() throws Exception{
		TreeMap<String,RawStream> rsMap=new TreeMap<String,RawStream>();
		EPStatementObjectModel som=epService.getEPAdministrator().compileEPL(epl);
		FromClause fc=som.getFromClause();
		for(com.espertech.esper.client.soda.Stream s: fc.getStreams()){
			if(s instanceof com.espertech.esper.client.soda.FilterStream){
				com.espertech.esper.client.soda.FilterStream fs=(com.espertech.esper.client.soda.FilterStream)s;
				String eventTypeName=fs.getFilter().getEventTypeName();
				for(RawStream rs: rawStreamList){
					if(rs.getEvent().getName().equals(eventTypeName)){
						rsMap.put(eventTypeName, rs);
						break;
					}
				}
			}
			else{
				throw new RuntimeException("not implemented yet");
			}
		}
		return rsMap.values();
	}
	
	public void start(){
		StringBuilder sb=new StringBuilder();
		if(epStatement!=null && !epStatement.isDestroyed()){
			epStatement.destroy();
		}
		//System.out.format("** start %s(id=%d)(eqlId=%d)\n", );
		try{
			epl=epl.trim();
			if(epl.endsWith(";")){
				epl=epl.substring(0, epl.length()-1);
			}
			this.epl = epl + String.format(" output every %d msec", 
					ServiceManager.getInstance(workerId).getOutputIntervalUS()/1000);
			System.out.format("Worker %s will start epl: %s\n", this.workerId, epl);
			epStatement=epService.getEPAdministrator().createEPL(epl);
			epStatement.addListener(epListener);
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
		for(Subscriber sub: subList){
			sub.start();
		}
	}
	
	public void stop(){
		
	}
	
	public void resume(){
		
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}
	
	public long getId() {
		return id;
	}

	@Override
	public void updateSubscriberObserver(long subscriberId, String streamName,
			String[] elementNames, String eventTypeName, Object[] events) {
		if(events[0] instanceof Map<?,?>){			
			for(Object event: events){
				epService.getEPRuntime().sendEvent((Map<String,Object>)event, eventTypeName);
			}
		}
	}
	
	class EPListener implements UpdateListener{
		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			log.info("epl %d output %d events", eplId, (newEvents==null)?0:newEvents.length);
		}
	}
}
