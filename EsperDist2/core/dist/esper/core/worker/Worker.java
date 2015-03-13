package dist.esper.core.worker;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EventType;
import com.espertech.esper.event.property.Property;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.comm.LinkManager.NewLinkListener;
import dist.esper.core.cost.WorkerStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.*;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.*;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.WorkerStatCollector2.*;
import dist.esper.core.worker.pubsub.Instance;
import dist.esper.core.worker.pubsub.ProcessingScheduler2;
import dist.esper.core.worker.pubsub.Processor;
import dist.esper.core.worker.pubsub.PublishingScheduler2;
import dist.esper.core.worker.pubsub.RawStreamSampler;
import dist.esper.core.worker.pubsub.Subscriber;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator.EPSRelation;
import dist.esper.epl.expr.util.EventOrPropertySpecReferenceDumper;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;
import dist.esper.event.EventRegistry;


import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;


public class Worker {
	static Logger2 log=Logger2.getLogger(Worker.class);
	public String id;
	public Link coordLink=null;
	public LinkManager linkManager;
	public EPServiceProvider epService;
	//MappedEventRegistry eventRegistry=null;
	public Map<String,Stream> slMap=new HashMap<String,Stream>();
	public Map<String,Instance> insMap=new HashMap<String,Instance>();
	public WorkerLinkHandler workerLinkHandler=new WorkerLinkHandler();
	public CoordinatorLinkHandler coordLinkHandler=new CoordinatorLinkHandler();
	
	public RawStreamSampler rawSampler;
	public WorkerStatCollector2 workerStatCollector;
	ProcessingScheduler2 procScheduler;
	PublishingScheduler2 pubScheduler;

	WorkerStatReportor coordReportor;
	
	ReentrantReadWriteLock instancesLock=new ReentrantReadWriteLock();
	
	public static EventOrPropertySpecComparator epsComparator=new EventOrPropertySpecComparator();
	
	class CoordinatorLinkHandler implements Link.Listener{
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {}

		@Override
		public void received(Link link, Object obj) {
			handleCoordinatorMessage(obj);
		}
	}
	
	class WorkerLinkHandler implements NewLinkListener, Link.Listener{
		List<Link> workerLinkList=new ArrayList<Link>();
		@Override
		public void newReceivedLink(Link link) {
			workerLinkList.add(link);
			link.addListener(this, StartSubscribeMessage.class.getSimpleName());
			link.addListener(this, ResumeSubscribeMessage.class.getSimpleName());
			log.info("Worker %s accept link(%s) from %s", id, link.getLinkId(), link.getTargetId().getId());
		}
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {
			link.removeListener(this, StartSubscribeMessage.class.getSimpleName());
			link.removeListener(this, ResumeSubscribeMessage.class.getSimpleName());
			workerLinkList.remove(link);
		}
	
		@Override
		public void received(Link link, Object obj) {
			handleWorkerMessage(link, obj);
		}
	}	
	
	public Worker(String id) {
		super();
		this.id = id;
	}	
	
	public void init(){
		epService=EPServiceProviderManager.getProvider(id);
		procScheduler=new ProcessingScheduler2(id);
		pubScheduler=new PublishingScheduler2(id);
		
		linkManager=ServiceManager.getInstance(id).getLinkManager();
		linkManager.init();
		linkManager.setNewLinkListener(workerLinkHandler);
		
		workerStatCollector=new WorkerStatCollector2(this, procScheduler, pubScheduler);
		
		coordLink=linkManager.connect(ServiceManager.getCoordinatorWorkerId());
		coordLink.addListener(coordLinkHandler);
		coordLink.send(new NewWorkerMessage());
		
		rawSampler=new RawStreamSampler(linkManager);
		rawSampler.start();

		coordReportor=new WorkerStatReportor(this);
	}
	public void start(){
		start(true);
	}
	public void start(boolean sync){
		if(sync){		
			coordReportor.run();
		}
		else{
			new Thread(coordReportor).start();
		}
		log.info("Worker %s is running...", this.id);
	}
	
	public void registEventRecursively(Event event){
		for(EventProperty prop: event.getPropList()){
			if(!prop.isArray() && (prop.getComponentType() instanceof Event)){
				registEventRecursively((Event)prop.getComponentType());
			}
		}
		for(EventProperty prop: event.getPropList()){
			if(prop.isArray() && (prop.getComponentType() instanceof Event)){
				registEventRecursively((Event)prop.getComponentType());
			}
		}
		registExplicitEvent(event);
	}
	
	public void registExplicitEvent(Event event){
		event.refresh();
		ServiceManager.getInstance(id).getEventRegistry().registEvent(event);
		epService.getEPAdministrator().getConfiguration().addEventType(event.getName(), event);
		
//		EventType eventType=epService.getEPAdministrator().getConfiguration().getEventType(event.getName());
//		String[] propNames=eventType.getPropertyNames();
//		System.out.format("** registed event: %s:%s\n", eventType.getName(), Arrays.asList(propNames).toString());
	}
	
	public void handleWorkerMessage(Link link, Object obj){
		if(obj instanceof StartSubscribeMessage){
			StartSubscribeMessage subMsg=(StartSubscribeMessage)obj;			
			Instance instance=getInstanceByStreamName(subMsg.getStreamName());			
			if(instance==null){
				log.error("instance is null, name is %s", subMsg.getStreamName());
				return;
			}
			instance.createPublisher(link, subMsg.getStreamName(),
					subMsg.getSelectElementNameList());
			workerStatCollector.updateWorkerStat(Worker.this);
		}
		else if(obj instanceof ResumeSubscribeMessage){
			ResumeSubscribeMessage resubMsg=(ResumeSubscribeMessage)obj;			
			Instance instance=getInstanceByStreamName(resubMsg.getStreamName());			
			instance.modifyPublisher(link, resubMsg.getStreamName(),						
					resubMsg.getSelectElementNameList());
		}
	}
	
	private Instance getInstanceByStreamName(String streamName){
		Instance ins=null;
		while(ins==null){
			instancesLock.readLock().lock();
			ins=insMap.get(streamName);
			instancesLock.readLock().unlock();
			try{Thread.sleep(1000);}catch(Exception ex){}
		}
		return ins;
	}

	public void handleCoordinatorMessage(Object obj){
		if(obj instanceof NewStreamInstanceMessage){
			NewStreamInstanceMessage nsiMsg=(NewStreamInstanceMessage)obj;
			handleNewStreamContainer(nsiMsg.getStreamContainer());
			workerStatCollector.updateWorkerStat(this);
		}
		else if(obj instanceof ModifyStreamInstanceMessage){
			ModifyStreamInstanceMessage msiMsg=(ModifyStreamInstanceMessage)obj;
			modifyStreamContainer(msiMsg.getStreamContainer());
		}
		else if(obj instanceof NewRawStreamSamplingMessage){
			NewRawStreamSamplingMessage nrssMsg=(NewRawStreamSamplingMessage)obj;
			sampleNewRawStream(nrssMsg.getRawStream());
			workerStatCollector.updateWorkerStat(this);
		}
		else if(obj instanceof GatewayRoleMessage){
			GatewayRoleMessage grMsg=(GatewayRoleMessage)obj;
			workerStatCollector.setGateway(grMsg.isGateway());
		}
	}
	
	public void sampleNewRawStream(RawStream rsl){
		rawSampler.sampleNewRawStream(rsl);
	}
	
	public void modifyStreamContainer(StreamContainer sc){
		Instance instance=insMap.get(sc.getUniqueName());
		assert(instance!=null);
		if(sc instanceof RootStreamContainer){
			handleRootStreamContainer((RootStreamContainer)sc);
		}
		else if(sc instanceof JoinDelayedStreamContainer){
			handleJoinDelayedStreamContainer((JoinDelayedStreamContainer)sc);			
		}
		else if(sc instanceof JoinStreamContainer){
			handleJoinStreamContainer((JoinStreamContainer)sc);
		}
		else if(sc instanceof FilterDelayedStreamContainer){
			handleFilterDelayedStreamContainer((FilterDelayedStreamContainer)sc);			
		}
		else if(sc instanceof FilterStreamContainer){
			handleFilterStreamContainer((FilterStreamContainer)sc);
		}
		else if(sc instanceof PatternStreamContainer){
			handlePatternStreamContainer((PatternStreamContainer)sc);
		}
		instance.stop();
		instance.modify((DerivedStreamContainer)sc);
		instance.resume();
	}
	
	public void handleNewStreamContainer(StreamContainer sc){
		instancesLock.writeLock().lock();
		Instance instance=null;
		if(sc instanceof RootStreamContainer){
			handleRootStreamContainer((RootStreamContainer)sc);			
		}
		else if(sc instanceof JoinDelayedStreamContainer){
			handleJoinDelayedStreamContainer((JoinDelayedStreamContainer)sc);			
		}
		else if(sc instanceof JoinStreamContainer){
			handleJoinStreamContainer((JoinStreamContainer)sc);			
		}
		else if(sc instanceof FilterDelayedStreamContainer){
			handleFilterDelayedStreamContainer((FilterDelayedStreamContainer)sc);			
		}
		else if(sc instanceof FilterStreamContainer){
			handleFilterStreamContainer((FilterStreamContainer)sc);			
		}
		else if(sc instanceof PatternStreamContainer){
			handlePatternStreamContainer((PatternStreamContainer)sc);			
		}
		instance=new Instance(id, (DerivedStreamContainer)sc, linkManager, epService, 
				ServiceManager.getInstance(id).getEventRegistry(),
				procScheduler, pubScheduler, 
				workerStatCollector.getSubscriberInspector(),
				workerStatCollector.getProcessorInspector(),
				workerStatCollector.getPublisherInspector());
		slMap.put(sc.getUniqueName(), sc);
		insMap.put(sc.getUniqueName(), instance);
		
		instance.init();
		instance.start();
		instancesLock.writeLock().unlock();
	}
	
	public void handleFilterDelayedStreamContainer(FilterDelayedStreamContainer fcsc){
		//registEventRecursively(fsl.getRawStreamLocation().getCompositeEvent());
		InternalCompositeEventGenerator.genInternalCompositeEvent(fcsc.getAgent());
		Set<EventOrPropertySpecification> agentEpsSet=fcsc.getAgent().dumpResultEventOrPropertySpecReferences();
		
		Set<EventOrPropertySpecification> extraCondEpsSet=EventOrPropertySpecReferenceDumper.dump(fcsc.getExtraFilterCondList(), null);
		
		for(EventOrPropertySpecification extraJoinEps: extraCondEpsSet){
			for(EventOrPropertySpecification agentEps: agentEpsSet){
				EPSRelation r=epsComparator.compare(extraJoinEps, agentEps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					try {
						NickNameGenerator.linkNickName(extraJoinEps, agentEps);
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		Set<EventOrPropertySpecification> joinResultEpsSet=fcsc.dumpResultEventOrPropertySpecReferences();
		for(EventOrPropertySpecification joinResultEps: joinResultEpsSet){
			for(EventOrPropertySpecification agentEps: agentEpsSet){
				EPSRelation r=epsComparator.compare(joinResultEps, agentEps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					try {
						NickNameGenerator.linkNickName(joinResultEps, agentEps);
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		registEventRecursively(fcsc.getAgent().getInternalCompositeEvent());
	}
	
	public void handleFilterStreamContainer(FilterStreamContainer fsl){
		registEventRecursively(fsl.getRawStream().getInternalCompositeEvent());
	}
	
	public void handleRootStreamContainer(RootStreamContainer rsl){
		InternalCompositeEventGenerator.genInternalCompositeEvent(rsl.getUpContainer());
		Set<EventOrPropertySpecification> childEpsSet=rsl.getUpContainer().dumpResultEventOrPropertySpecReferences();
		
		Set<EventOrPropertySpecification> whereEpsSet=EventOrPropertySpecReferenceDumper.dump(rsl.getWhereExprList(), null);

		for(EventOrPropertySpecification whereEps: whereEpsSet){
			for(EventOrPropertySpecification childEps: childEpsSet){
				EPSRelation r=epsComparator.compare(whereEps, childEps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					try {
						NickNameGenerator.linkNickName(whereEps, childEps);
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		
		Set<EventOrPropertySpecification> rootResultEpsSet=rsl.dumpResultEventOrPropertySpecReferences();
		for(EventOrPropertySpecification rootResultEps: rootResultEpsSet){
			for(EventOrPropertySpecification childEps: childEpsSet){
				EPSRelation r=epsComparator.compare(rootResultEps, childEps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					try {
						NickNameGenerator.linkNickName(rootResultEps, childEps);
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		registEventRecursively(rsl.getUpContainer().getInternalCompositeEvent());
	}	

	public void handlePatternStreamContainer(PatternStreamContainer psl){
		for(RawStream child: psl.getRawStreamList()){
			registEventRecursively(child.getInternalCompositeEvent());
		}
	}
	
	public void handleJoinDelayedStreamContainer(JoinDelayedStreamContainer jcsc){
		InternalCompositeEventGenerator.genInternalCompositeEvent(jcsc.getAgent());
		Set<EventOrPropertySpecification> agentEpsSet=jcsc.getAgent().dumpResultEventOrPropertySpecReferences();
		
		Set<EventOrPropertySpecification> extraCondEpsSet=new HashSet<EventOrPropertySpecification>();
		EventOrPropertySpecReferenceDumper.dump(jcsc.getExtraJoinCondList(), extraCondEpsSet);
		EventOrPropertySpecReferenceDumper.dump(jcsc.getExtraChildCondList(), extraCondEpsSet);
		
		
		for(EventOrPropertySpecification extraJoinEps: extraCondEpsSet){
			for(EventOrPropertySpecification agentEps: agentEpsSet){
				EPSRelation r=epsComparator.compare(extraJoinEps, agentEps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					try {
						NickNameGenerator.linkNickName(extraJoinEps, agentEps);
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		Set<EventOrPropertySpecification> joinResultEpsSet=jcsc.dumpResultEventOrPropertySpecReferences();
		for(EventOrPropertySpecification joinResultEps: joinResultEpsSet){
			for(EventOrPropertySpecification agentEps: agentEpsSet){
				EPSRelation r=epsComparator.compare(joinResultEps, agentEps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					try {
						NickNameGenerator.linkNickName(joinResultEps, agentEps);
					}
					catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		registEventRecursively(jcsc.getAgent().getInternalCompositeEvent());
	}
	
	public void handleJoinStreamContainer(JoinStreamContainer jsl){
		//List<Event> childEventList=new ArrayList<Event>(jsl.getChildList().size());
		List<Set<EventOrPropertySpecification>> childEpsSetList=new ArrayList<Set<EventOrPropertySpecification>>(jsl.getUpContainerList().size());
		for(StreamContainer child: jsl.getUpContainerList()){
			//NickNameGenerator.genNickNameForEventPropertySpecs(child);
			InternalCompositeEventGenerator.genInternalCompositeEvent(child);
			Set<EventOrPropertySpecification> epsSet=child.dumpResultEventOrPropertySpecReferences();
			childEpsSetList.add(epsSet);
		}
				
		Set<EventOrPropertySpecification> joinEpsSet=EventOrPropertySpecReferenceDumper.dump(jsl.getJoinExprList(), null);
				
		for(EventOrPropertySpecification joinEps: joinEpsSet){
			boolean flag=false;
			for(int i=0; i<childEpsSetList.size(); i++){
				for(EventOrPropertySpecification childEps: childEpsSetList.get(i)){
					EPSRelation r=epsComparator.compare(joinEps, childEps);
					if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
						try {
							NickNameGenerator.linkNickName(joinEps, childEps);
						}
						catch (Exception e) {
							e.printStackTrace();
						}
						flag=true;
						break;
					}	
				}
			}
			assert(flag);
			if(!flag){
				System.out.print("");
			}
		}
		
		Set<EventOrPropertySpecification> joinResultEpsSet=jsl.dumpResultEventOrPropertySpecReferences();
		for(EventOrPropertySpecification joinResultEps: joinResultEpsSet){
			for(int i=0; i<childEpsSetList.size(); i++){
				for(EventOrPropertySpecification childEps: childEpsSetList.get(i)){
					EPSRelation r=epsComparator.compare(joinResultEps, childEps);
					if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
						try {
							NickNameGenerator.linkNickName(joinResultEps, childEps);
						}
						catch (Exception e) {
							e.printStackTrace();
						}
						break;
					}
				}
			}
		}
		for(Stream child: jsl.getUpContainerList()){
			registEventRecursively(child.getInternalCompositeEvent());
		}
		System.out.print("");
	}
	
	@Override
	public String toString(){
		return this.getClass().getSimpleName()+"["+id+"]";
	}
	
	static class InternalCompositeEventGenerator{
		public static AtomicLong counter=new AtomicLong(0L);
		
		public static Event genInternalCompositeEvent(StreamContainer sl){//sl won't be RootStreamLocation currently
			long seq=counter.getAndIncrement();
			
			Event event=new Event("CE_"+seq);
			//sl.setCompositeEvent(event);
			/*NickNameGenerator.genNickNameForEventPropertySpecs(sl);*/
			
			for(SelectClauseExpressionElement se: sl.getResultElementList()){
				assert(se.getSelectExpr() instanceof EventOrPropertySpecification);
				EventOrPropertySpecification eps=(EventOrPropertySpecification)se.getSelectExpr();		

				eps.setInternalEventNickName(sl.getUniqueName());
				eps.setInternalEventPropertyName(se.getUniqueName());
				if(eps instanceof EventPropertySpecification){
					if(eps instanceof EventPropertyIndexedSpecification){
						EventPropertyIndexedSpecification epis=(EventPropertyIndexedSpecification)eps;
						Object type=epis.getEventProp().getComponentType();
						event.addProperty(epis.getInternalEventPropertyName(), type);//FIXME: eps.nickName=se.uniqueName=event.property.name;
					}
					else{
						EventPropertySpecification eps1=(EventPropertySpecification)eps;
						event.addProperty(eps1.getInternalEventPropertyName(), eps1.getEventProp().getType());//array will also be array
					}
				}
				else{
					if(eps instanceof EventIndexedSpecification){
						EventIndexedSpecification eis=(EventIndexedSpecification)eps;
						event.addProperty(eis.getInternalEventPropertyName(), eis.getEventAlias().getEvent());
					}
					else{
						EventSpecification eis=(EventSpecification)eps;
						if(!eis.isArray()){
							event.addProperty(eis.getInternalEventPropertyName(), eis.getEventAlias().getEvent());
						}
						else{
							event.addProperty(eis.getInternalEventPropertyName(), eis.getEventAlias().getEvent().getName()+"[]");
						}
					}
				}
			}
			
			sl.setInternalCompositeEvent(event);
			return event;
		}
	}
	
	static class NickNameGenerator{		
		//sub is contained by base
		public static void linkNickName(EventOrPropertySpecification sub, EventOrPropertySpecification base) throws Exception{
			String subName=sub.toString();
			String baseName=base.toString();
			if(!subName.startsWith(baseName)){
				throw new Exception(String.format("'%s' is not contained by '%s'", subName, baseName));
			}
			String subNickName=base.getInternalEventPropertyName() + subName.substring(baseName.length());
			sub.setInternalEventPropertyName(subNickName);
			sub.setInternalEventNickName(base.getInternalEventNickName());
		}
	}
}
