package dist.esper.core.worker.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.PropertyAccessException;

import dist.esper.core.comm.Link;
import dist.esper.core.cost.InstanceStat;
import dist.esper.core.message.DataMessage;
import dist.esper.util.Logger2;

public class Publisher implements IProcessorObserver{
	static Logger2 log=Logger2.getLogger(Publisher.class);
	String workerId;
	long id;
	Link link=null;
	String streamName;
	ReentrantReadWriteLock rwLock=new ReentrantReadWriteLock();
	String[] selectElementNames;
	InstanceStat instanceStat;
	PublishDelegator pubDelegator;
	static AtomicLong UID=new AtomicLong(0L);
	
	public boolean isLocalPublisher(){
		return workerId.equals(link.getTargetId().getId());
	}	
	
	public PublishDelegator getPublishDelegator() {
		return pubDelegator;
	}

	public void setPublishDelegator(PublishDelegator pubDelegator) {
		this.pubDelegator = pubDelegator;
	}
	
	public String getDestinationWorkerId(){
		return link.getTargetId().getId();
	}

	public Publisher(String workerId, Link link, String streamName, 
			//String streamEventTypeName, 
			List<String> selectElementNameList,
			InstanceStat instanceStat) {
		super();
		this.workerId = workerId;
		this.link = link;
		this.streamName = streamName;
		//this.streamEventTypeName = streamEventTypeName;
		//this.selectElementNameList = selectElementNameList;
		this.setSelectElementNames(selectElementNameList);
		this.instanceStat = instanceStat;
		this.id=UID.getAndIncrement();
	}
	
	public void modify(List<String> additionSelectElementNameList){//for the same link
		List<String> newSelectElementNameList=new ArrayList<String>(selectElementNames.length 
				+ additionSelectElementNameList.size());
		newSelectElementNameList.addAll(additionSelectElementNameList);
		for(String oldName: selectElementNames){
			if(!newSelectElementNameList.contains(oldName)){
				newSelectElementNameList.add(oldName);
			}
		}
		this.setSelectElementNames(newSelectElementNameList);
	}

	public Link getLink() {
		return link;
	}

	public void setLink(Link link) {
		this.link = link;
	}

	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}
	
//	public String getStreamEventTypeName() {
//		return streamEventTypeName;
//	}
//
//	public void setStreamEventTypeName(String streamEventTypeName) {
//		this.streamEventTypeName = streamEventTypeName;
//	}

	public String[] getSelectElementNames() {
		return selectElementNames;
	}

	public void setSelectElementNames(String[] selectElementNames) {
		this.selectElementNames = selectElementNames;
	}
	
	public void setSelectElementNames(List<String> selectElementNameList) {
		rwLock.writeLock().lock();{
			this.selectElementNames=new String[selectElementNameList.size()];
			this.selectElementNames = selectElementNameList.toArray(this.selectElementNames);
		}rwLock.writeLock().unlock();
	}
	
	public InstanceStat getInstanceStat() {
		return instanceStat;
	}

	public void setInstanceStat(InstanceStat instanceStat) {
		this.instanceStat = instanceStat;
	}

	@Override
	public void updateProcessorObserver(EventBean[] newEvents) {
//		if(pubDelegator!=null){
//			pubDelegator.publish(newEvents, this);
//		}		
		long startSerialTimeNS=System.nanoTime();
		Object[][] records=new Object[newEvents.length][];
		DataMessage dataMsg=null;
		try{
			rwLock.readLock().lock();
			for(int i=0; i<newEvents.length; i++){
				records[i]=new Object[selectElementNames.length];
				for(int j=0; j<selectElementNames.length; j++){
					records[i][j]=newEvents[i].get(selectElementNames[j]);
				}
			}
			dataMsg=new DataMessage(workerId, streamName, 
					selectElementNames,
					records);
		}
		catch(PropertyAccessException ex){
			log.error("error occur when get selected elements", ex);
			return;
		}
		finally{
			rwLock.readLock().unlock();
		}
		long endSerialTimeNS=System.nanoTime();
		long startSendTimeNS=endSerialTimeNS;
		link.send(dataMsg);
		long endSendTimeNS=System.nanoTime();
		
		instanceStat.updatePublisherStat(this.id,
				newEvents.length, //deltaOuputEventCount, 
				(endSerialTimeNS-startSerialTimeNS)/1000.0, //deltaSerialTimeUS, 
				(endSendTimeNS-startSendTimeNS)/1000.0, //deltaOutputTimeUS, 
				Link.LOCAL_TRANSMISSION);//deltaOuputBytes
		
	}
	
	public static interface PublishDelegator{
		public void publish(EventBean[] newEvents, Publisher pub);
	}

	public long getId() {
		return id;
	}
	
}
