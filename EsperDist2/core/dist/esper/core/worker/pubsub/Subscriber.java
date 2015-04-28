package dist.esper.core.worker.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.cost.InstanceStat;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.DataMessage;
import dist.esper.core.message.ResumeSubscribeMessage;
import dist.esper.core.message.StartSubscribeMessage;
import dist.esper.core.worker.pubsub.Processor.State;
import dist.esper.event.Event;
import dist.esper.util.Logger2;
import dist.esper.util.Tuple4D;

import com.esotericsoftware.kryonet.Listener;

public class Subscriber{
	static Logger2 log=Logger2.getLogger(Subscriber.class);
	String workerId;
	long id;
	Link link=null;
	LinkManager linkManager=null;
	
	State state=State.NONE;
	WorkerId targetId=null;
	volatile String streamName=null;
	volatile String internalEventTypeName=null;	
	volatile List<String> elementNameList=null;	
	ISubscriberObserver proc=null;
	SubscribeRunnable runner=new SubscribeRunnable();
	LinkHandler linkHandler=new LinkHandler();
	InstanceStat instanceStat;
	ProcessingScheduler2 procScheduler;
	long windowTimeUS=0;
	ReentrantReadWriteLock subModifyLock=new ReentrantReadWriteLock();
	static AtomicLong UID=new AtomicLong(0L);
	
	class LinkHandler implements Link.Listener{
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {}

		@Override
		public void received(Link link, Object obj) {
			if(obj instanceof DataMessage){
				DataMessage dataMsg=(DataMessage)obj;
				if(state!=State.STOPPED){
					if(streamName.equals(dataMsg.getStreamName()) 
							//&& streamEventTypeName.equals(dataMsg.getEventTypeName())
							){
						updateObservers(dataMsg.getData(), dataMsg.getElementNames());
					}
				}
			}
		}
	}
	public InstanceStat getInstanceStat() {
		return instanceStat;
	}

	public void setInstanceStat(InstanceStat instanceStat) {
		this.instanceStat = instanceStat;
	}
	
	public boolean isLocalSubscriber(){
		return workerId.equals(targetId.getId());
	}
	
	public String getSourceWorkerId(){
		return targetId.getId();
	}
	
	public long getWindowTimeUS() {
		return windowTimeUS;
	}

	public void setWindowTimeUS(long windowTimeUS) {
		this.windowTimeUS = windowTimeUS;
	}
	
	public Subscriber(String workerId, String streamName, String streamEventTypeName, 
			List<String> elementNameList, long windowTimeUS, WorkerId targetId, 
			LinkManager linkManager, ProcessingScheduler2 procScheduler, 
			InstanceStat instanceStat) {
		super();
		this.workerId = workerId;		
		this.targetId = targetId;
		this.streamName = streamName;
		this.internalEventTypeName = streamEventTypeName;		
		this.elementNameList = elementNameList;
		this.windowTimeUS = windowTimeUS;
		this.linkManager = linkManager;
		this.procScheduler = procScheduler;
		this.instanceStat = instanceStat;
		this.id = UID.getAndIncrement();
	}
	
	public void modify(String newStreamEventTypeName, List<String> newElementNameList){
		assert(this.state==State.STOPPED);
		log.info("Subscriber %d for upStream %s is modified: old internalEventTypeName=%s, new internalEventTypeName=%s, "
				+ "old elementNameList.length=%d, new elementNameList.length=%d",
				id, streamName, this.internalEventTypeName, newStreamEventTypeName,
				this.elementNameList.size(), newElementNameList.size());
		subModifyLock.writeLock().lock();
		this.internalEventTypeName=newStreamEventTypeName;
		this.elementNameList=newElementNameList;
		this.state=State.INITTED;
		subModifyLock.writeLock().unlock();
		
	}
	
	public void init(){
		link=linkManager.connect(targetId);
		link.addListener(linkHandler, DataMessage.class.getSimpleName());
		this.state=State.INITTED;
	}
	
	public void start(){
		StartSubscribeMessage subMsg=new StartSubscribeMessage(
				link.getMyId().getId(),
				streamName,
				internalEventTypeName,
				elementNameList);
		link.send(subMsg);
		this.state=State.RUNNING;
	}
	
	public void stop(){
		this.state=State.STOPPED;
		//FIXME: send stop subscribe message
	}
	
	public void resume(){
		ResumeSubscribeMessage resubMsg=new ResumeSubscribeMessage(
				link.getMyId().getId(),
				streamName,
				internalEventTypeName,
				elementNameList);
		link.send(resubMsg);
		this.state=State.RUNNING;
	}
	
	public long getId() {
		return id;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public String getInternalEventTypeName() {
		return internalEventTypeName;
	}

	public void setInternalEventTypeName(String streamEventTypeName) {
		this.internalEventTypeName = streamEventTypeName;
	}
	
	public void setObserver(ISubscriberObserver proc){
		this.proc = proc;
	}
	
	public ISubscriberObserver getObserver(){
		return proc;
	}
	
	public void updateObservers(Object[] events, String[] elementNames){
		if(state!=State.RUNNING){
			log.error("** subscriber receives %s Objects in updateObserver, but is not in RUNNING STATE", events.length);
			return;
		}
		if(proc!=null){
			subModifyLock.readLock().lock();
			procScheduler.sumbit(this.id, streamName, elementNames, internalEventTypeName, events, proc);
			subModifyLock.readLock().unlock();
		}
	}
	
	
	class SubscribeRunnable implements Runnable{
		@Override
		public void run() {		
		}		
	}
	
	public enum State{
		NONE("none"),
		INITTED("inited"),
		CONNECTING("connecting"),
		CONNECTED("connected"),
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
