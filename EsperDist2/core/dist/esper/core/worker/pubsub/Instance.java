package dist.esper.core.worker.pubsub;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import com.espertech.esper.client.EPServiceProvider;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.cost.InstanceStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.*;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.WorkerStatCollector2.ProcessorInspector;
import dist.esper.core.worker.WorkerStatCollector2.PublisherInspector;
import dist.esper.core.worker.WorkerStatCollector2.SubscriberInspector;
import dist.esper.core.worker.pubsub.Processor.State;
import dist.esper.event.EventRegistry;
import dist.esper.util.Logger2;

public class Instance {
	static Logger2 log=Logger2.getLogger(Instance.class);
	String workerId;
	long id;
	LinkManager linkManager;
	EPServiceProvider epService;
	//SubscriberFactory subFactory;
	EventRegistry eventRegistry;	
	List<Subscriber> subList=new ArrayList<Subscriber>(2);
	Processor proc=null;
	List<Publisher> pubList=new ArrayList<Publisher>(2);
	DerivedStreamContainer streamContainer=null;
	State state=State.NONE;
	InstanceStat instanceStat;
	ProcessingScheduler2 procScheduler;
	PublishingScheduler2 pubScheduler;
	static AtomicLong UID=new AtomicLong(0L);
	
	public void init(){
		proc.init();
		for(Subscriber sub: subList){
			sub.init();
		}
		this.state=State.INITTED;
	}
	
	public void start(){
		proc.start();
		for(Subscriber sub: subList){
			sub.start();
		}
		this.state=State.RUNNING;
	}
	
	public void stop(){
		for(Subscriber sub: subList){
			sub.stop();
		}
		proc.stop();
		this.state=State.STOPPED;
	}
	
	public void resume(){
		proc.resume();
		for(Subscriber sub: subList){
			sub.resume();
		}
		this.state=State.RUNNING;
	}
	
	public InstanceStat getInstanceStat() {
		return instanceStat;
	}

	public void setInstanceStat(InstanceStat instanceStat) {
		this.instanceStat = instanceStat;
		proc.setInstanceStat(instanceStat);
		for(Subscriber sub: subList){
			sub.setInstanceStat(instanceStat);
		}
		for(Publisher pub: pubList){
			pub.setInstanceStat(instanceStat);
		}
	}
	
	public DerivedStreamContainer getStreamContainer() {
		return streamContainer;
	}

	public void setStreamContainer(
			DerivedStreamContainer streamContainer) {
		this.streamContainer = streamContainer;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public Instance(String workerId, DerivedStreamContainer streamContainer,
			LinkManager linkManager, EPServiceProvider epService,
			EventRegistry eventRegistry,
			ProcessingScheduler2 procScheduler,
			PublishingScheduler2 pubScheduler,
			SubscriberInspector subInspector,
			ProcessorInspector procInspector,
			PublisherInspector pubInspector){
			//, SubscriberFactory subFactory) {
		super();
		this.workerId = workerId;
		this.streamContainer = streamContainer;
		this.linkManager = linkManager;
		this.epService = epService;
		this.eventRegistry = eventRegistry;
		//this.subFactory = subFactory;
		this.procScheduler = procScheduler;
		this.pubScheduler = pubScheduler;
		this.id=UID.getAndIncrement();
		
		buildProcessorAndSubscribers();
		InstanceStat insStat=new InstanceStat(this, subInspector, procInspector, pubInspector);
		this.setInstanceStat(insStat);
	}
	
	public void modify(DerivedStreamContainer newStreamContainer){
		assert(this.state==State.STOPPED);
		this.streamContainer=newStreamContainer;
		
		if(streamContainer instanceof RootStreamContainer){
			RootStreamContainer rsl=(RootStreamContainer)streamContainer;
			proc.modify(rsl);
			StreamContainer child=rsl.getUpContainer();
			Subscriber sub=subList.get(0);
			sub.modify(child.getInternalCompositeEvent().getName(), child.getResultElementUniqueNameList());
		}
		if(streamContainer instanceof JoinDelayedStreamContainer){
			JoinDelayedStreamContainer jcsl=(JoinDelayedStreamContainer)streamContainer;
			proc.modify(jcsl);
			StreamContainer agent=jcsl.getAgent();
			Subscriber sub=subList.get(0);
			sub.modify(agent.getInternalCompositeEvent().getName(), agent.getResultElementUniqueNameList());
		}
		else if(streamContainer instanceof JoinStreamContainer){
			JoinStreamContainer jsl=(JoinStreamContainer)streamContainer;
			proc.modify(jsl);
			int count=0;
			for(StreamContainer child: jsl.getUpContainerList()){
				for(Subscriber sub: subList){
					if(sub.getStreamName().equals(child.getUniqueName())){
						sub.modify(child.getInternalCompositeEvent().getName(), child.getResultElementUniqueNameList());
						count++;
						break;
					}
				}
			}
			assert(count==jsl.getUpContainerList().size());
		}
		if(streamContainer instanceof FilterDelayedStreamContainer){
			FilterDelayedStreamContainer fcsl=(FilterDelayedStreamContainer)streamContainer;
			proc.modify(fcsl);
			StreamContainer agent=fcsl.getAgent();
			Subscriber sub=subList.get(0);
			sub.modify(agent.getInternalCompositeEvent().getName(), agent.getResultElementUniqueNameList());
		}
		else if(streamContainer instanceof FilterStreamContainer){
			FilterStreamContainer fsl=(FilterStreamContainer)streamContainer;
			proc.modify(fsl);
		}
		else if(streamContainer instanceof PatternStreamContainer){
			PatternStreamContainer psl=(PatternStreamContainer)streamContainer;
			proc.modify(psl);
		}
		this.state=State.INITTED;
	}
	
	public void buildProcessorAndSubscribers(){
		proc=new Processor(workerId, epService,	eventRegistry, 
				streamContainer, pubScheduler, instanceStat);
		if(streamContainer instanceof RootStreamContainer){
			RootStreamContainer rsc=(RootStreamContainer)streamContainer;
			DerivedStreamContainer child=(DerivedStreamContainer)rsc.getUpContainer();
			Subscriber sub=new Subscriber(
					workerId,
					child.getUniqueName(),
					child.getInternalCompositeEvent().getName(),
					child.getResultElementUniqueNameList(),
					child.getWindowTimeUS(),
					child.getWorkerId(),
					linkManager,
					procScheduler,
					instanceStat);			
			this.addSubscriber(sub);
		}
		else if(streamContainer instanceof JoinDelayedStreamContainer){
			JoinDelayedStreamContainer jcsc=(JoinDelayedStreamContainer)streamContainer;
			Subscriber sub=new Subscriber(
					workerId, 
					jcsc.getAgent().getUniqueName(),
					jcsc.getAgent().getInternalCompositeEvent().getName(),
					jcsc.getAgent().getResultElementUniqueNameList(),
					jcsc.getAgent().getWindowTimeUS(),
					jcsc.getAgent().getWorkerId(),
					linkManager,
					procScheduler,
					instanceStat);
			this.addSubscriber(sub);
		}
		else if(streamContainer instanceof JoinStreamContainer){
			JoinStreamContainer jsc=(JoinStreamContainer)streamContainer;
			for(StreamContainer jscChild: jsc.getUpContainerList()){
				DerivedStreamContainer child=(DerivedStreamContainer)jscChild;
				Subscriber sub=new Subscriber(
						workerId, 
						child.getUniqueName(),
						child.getInternalCompositeEvent().getName(),
						child.getResultElementUniqueNameList(),
						child.getWindowTimeUS(),
						child.getWorkerId(),
						linkManager,
						procScheduler,
						instanceStat);
				this.addSubscriber(sub);
			}
		}
		else if(streamContainer instanceof FilterDelayedStreamContainer){
			FilterDelayedStreamContainer fcsc=(FilterDelayedStreamContainer)streamContainer;
			Subscriber sub=new Subscriber(
					workerId, 
					fcsc.getAgent().getUniqueName(), //WRONG:fcsc.getAgent().getCompositeEvent().getName(),
					fcsc.getAgent().getInternalCompositeEvent().getName(),
					fcsc.getAgent().getResultElementUniqueNameList(),
					fcsc.getAgent().getWindowTimeUS(),
					fcsc.getAgent().getWorkerId(),
					linkManager,
					procScheduler,
					instanceStat);
			this.addSubscriber(sub);
		}
		else if(streamContainer instanceof FilterStreamContainer){
			FilterStreamContainer fsc=(FilterStreamContainer)streamContainer;
			RawStream child=fsc.getRawStream();
			Subscriber sub=new Subscriber(
					workerId, 
					child.getInternalCompositeEvent().getName(),
					child.getInternalCompositeEvent().getName(),
					child.getResultElementUniqueNameList(),
					10*1000*1000,//FIXME
					child.getWorkerId(),
					linkManager,
					procScheduler,
					instanceStat);
			this.addSubscriber(sub);
		}
		else if(streamContainer instanceof PatternStreamContainer){
			PatternStreamContainer psc=(PatternStreamContainer)streamContainer;
			for(RawStream child: psc.getRawStreamList()){
				Subscriber sub=new Subscriber(
						workerId,
						child.getInternalCompositeEvent().getName(),
						child.getInternalCompositeEvent().getName(),
						child.getResultElementUniqueNameList(),
						10*1000*1000,
						child.getWorkerId(),
						linkManager,
						procScheduler,
						instanceStat);
				this.addSubscriber(sub);
			}
		}
//		instanceStat.resizeSubscriberCount(subList.size());
//		for(int i=0;i<subList.size();i++){
//			Subscriber sub=subList.get(i);
//			//instanceStat.setSubscriberStat(i, sub.getId(), sub.getSourceWorkerId(), sub.getStreamName());
//		}
	}

	public void addSubscriber(Subscriber sub){
		subList.add(sub);
		proc.registerSubcriber(sub);
	}
	
	public void addPublisher(Publisher pub){
		pubList.add(pub);
		pub.setInstanceStat(instanceStat);
		proc.addObserver(pub);
	}
	
	public void createPublisher(Link link, String streamName, 
			//String streamEventTypeName, 
			List<String> selectElementNameList){
		//check whether the publisher with the same link and streamName exists 
		if(modifyPublisher(link, streamName, selectElementNameList)){
			return;
		}
		else{
			Publisher pub=new Publisher(workerId, link, streamName, 
					//streamEventTypeName, 
					selectElementNameList,
					instanceStat);
			addPublisher(pub);
			instanceStat.addPublisherStat(pub.getId(), pub.getDestinationWorkerId());
		}
	}
	
	public boolean modifyPublisher(Link link, String streamName, 
			//String streamEventTypeName, 
			List<String> selectElementNameList){
		for(Publisher pub: pubList){
			if(pub.link==link && pub.streamName.equals(streamName)){
				pub.modify(selectElementNameList);
				return true;
			}
		}
		return false;
	}

	public Processor getProcessor() {
		return proc;
	}

//	public void setProcessor(Processor proc) {
//		this.proc = proc;
//	}

	public List<Subscriber> getSubList() {
		return subList;
	}

	public List<Publisher> getPubList() {
		return pubList;
	}
	
	public long getId() {
		return id;
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
