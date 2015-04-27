package dist.esper.core.cost;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.esotericsoftware.kryo.DefaultSerializer;

import dist.esper.core.flow.container.*;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.WorkerStatCollector2.ProcessorInspector;
import dist.esper.core.worker.WorkerStatCollector2.PublisherInspector;
import dist.esper.core.worker.WorkerStatCollector2.SubscriberInspector;
import dist.esper.core.worker.pubsub.Instance;
import dist.esper.core.worker.pubsub.Subscriber;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.util.Logger2;
import dist.esper.util.StringUtil;
import dist.esper.core.cost.io.InstanceStatSerializer;

@DefaultSerializer(value = InstanceStatSerializer.class)
public class InstanceStat implements Serializable{
	static Logger2 log=Logger2.getLogger(InstanceStat.class);
	private static final long serialVersionUID = 8726058215812361568L;
	String workerId;
	String type;
	String uniqueName;
	long instanceId;
	long processorId;
	long startTimestampUS;//us
	long lastTimestampUS;//us
	long batchCount=0;
	long eventCount=0;//output count
	double procTimeUS=0;//total us
	double outputIntervalUS=0;//us
	SubscriberStat[] subStats;
	PublisherStat[] pubStats;
	//PropertyStat[] fieldStats;//only filter is not empty
	
	transient Map<Long, PublisherStat> pubStatMap=new TreeMap<Long, PublisherStat>();
	
	transient SubscriberInspector subInspector;
	transient ProcessorInspector procInspector;
	transient PublisherInspector pubInspector;
	
	transient ReentrantReadWriteLock subLock=new ReentrantReadWriteLock();
	transient ReentrantReadWriteLock procLock=new ReentrantReadWriteLock();
	transient ReentrantReadWriteLock pubLock=new ReentrantReadWriteLock();
	
	public InstanceStat(){
		super();
	}
	
	public InstanceStat(Instance instance, SubscriberInspector subInspector,
			ProcessorInspector procInspector, PublisherInspector pubInspector){
		this.workerId = instance.getWorkerId();
		this.instanceId = instance.getId();
		this.processorId = instance.getProcessor().getId();
		this.uniqueName = instance.getStreamContainer().getUniqueName();
		this.outputIntervalUS = ServiceManager.getOutputIntervalUS();
		this.startTimestampUS = System.nanoTime()/1000;
		this.lastTimestampUS = startTimestampUS+1;
		this.batchCount = 0;
		this.eventCount = 0;
		this.subInspector = subInspector;
		this.procInspector = procInspector;
		this.pubInspector = pubInspector;
		
		this.type=instance.getStreamContainer().getClass().getSimpleName();
		initProcessorStat(instance);
		initSubscriberStats(instance);
		initPublisherStats(instance);
	}
	
	public double computeSelectFactor(){
		int subCountsProduct=1;
		for(SubscriberStat subStat: this.subStats){
			subCountsProduct*=subStat.getEventCount();
		}
		if(subCountsProduct<=0){
			return 0.0d;
		}
		else{
			return ((double)eventCount)/((double)subCountsProduct);
		}
	}
	
	private void initProcessorStat(Instance instance){
		DerivedStreamContainer psc=instance.getStreamContainer();
		int condCount=0;
		if(psc instanceof JoinStreamContainer){
			JoinStreamContainer jsc=(JoinStreamContainer)psc;
			condCount=AbstractBooleanExpression.getComparisonExpressionCount(jsc.getJoinExprList());
		}
		else if(psc instanceof FilterStreamContainer){
			FilterStreamContainer fsc=(FilterStreamContainer)psc;
			condCount=fsc.getFilterExpr().getComparisonExpressionCount();
		}
		
		long[] subIds=new long[instance.getSubList().size()];
		for(int i=0;i<subIds.length;i++){
			subIds[i]=instance.getSubList().get(i).getId();
		}		
		procInspector.addProcessor(instance.getProcessor().getId(), subIds, condCount);
	}
	
	private void initSubscriberStats(Instance instance){
		List<Subscriber> subList=instance.getSubList();
		subStats=new SubscriberStat[subList.size()];
		for(int i=0;i<subList.size();i++){
			Subscriber sub=subList.get(i);
			subStats[i]=new SubscriberStat(sub.getId(), sub.getSourceWorkerId(), sub.getStreamName(), sub.getWindowTimeUS());
			subInspector.addSubscriber(sub.getId(), sub.getWindowTimeUS());
		}
		if(subList.size()>=2){
			subInspector.addSubscriberJoinPair(subList.get(0).getId(), subList.get(1).getId());
		}
	}
	
	public void initPublisherStats(Instance instance){
		this.pubStats = new PublisherStat[0];
	}
	
	public static class SubscriberStat implements Serializable{
		private static final long serialVersionUID = 1L;
		long subscriberId;
		String srcWorkerId;
		String uniqueName;
		long windowTimeUS;
		long startTimestampUS;//us
		long lastTimestampUS;//us
		long batchCount;
		long eventCount;
		public SubscriberStat(){
		}
		public SubscriberStat(long subscriberId, String srcWorkerId, String uniqueName, long windowTimeUS) {
			this(subscriberId, srcWorkerId, uniqueName);
			this.windowTimeUS = windowTimeUS;
		}
		public SubscriberStat(long subscriberId, String srcWorkerId, String uniqueName) {
			super();
			this.subscriberId = subscriberId;
			this.srcWorkerId = srcWorkerId;
			this.uniqueName = uniqueName;
			this.startTimestampUS = System.nanoTime()/1000;
			this.lastTimestampUS = startTimestampUS;
		}
		
		public void toStringBuilder(StringBuilder sb, int indent){
			sb.append(StringUtil.getSpaces(indent));
			sb.append("SubscriberStat[");
			sb.append("srcWorkerId="); sb.append(srcWorkerId);
			sb.append(", uniqueName="); sb.append(uniqueName);
			//sb.append(", startTimestampUS="); sb.append(startTimestampUS);
			//sb.append(", lastTimestampUS="); sb.append(lastTimestampUS);
			sb.append(", batchCount="); sb.append(batchCount);
			sb.append(", eventCount="); sb.append(eventCount);
			sb.append("]");
		}
		public long getSubscriberId() {
			return subscriberId;
		}
		public void setSubscriberId(long subscriberId) {
			this.subscriberId = subscriberId;
		}
		public String getSrcWorkerId() {
			return srcWorkerId;
		}
		public void setSrcWorkerId(String srcWorkerId) {
			this.srcWorkerId = srcWorkerId;
		}
		public String getUniqueName() {
			return uniqueName;
		}
		public void setUniqueName(String uniqueName) {
			this.uniqueName = uniqueName;
		}
		public long getWindowTimeUS() {
			return windowTimeUS;
		}
		public void setWindowTimeUS(long windowTimeUS) {
			this.windowTimeUS = windowTimeUS;
		}
		public long getStartTimestampUS() {
			return startTimestampUS;
		}
		public void setStartTimestampUS(long startTimestampUS) {
			this.startTimestampUS = startTimestampUS;
		}
		public long getLastTimestampUS() {
			return lastTimestampUS;
		}
		public void setLastTimestampUS(long lastTimestampUS) {
			this.lastTimestampUS = lastTimestampUS;
		}
		public long getBatchCount() {
			return batchCount;
		}
		public void setBatchCount(long batchCount) {
			this.batchCount = batchCount;
		}
		public long getEventCount() {
			return eventCount;
		}
		public void setEventCount(long eventCount) {
			this.eventCount = eventCount;
		}		
	}
	
	public static class PublisherStat implements Serializable{
		private static final long serialVersionUID = 1L;
		long publisherId;
		String destWorkerId;		
		long startTimestampUS;//us
		long lastTimestampUS;//us
		double serialTimeUS=0;//us
		double outputTimeUS=0;//us
		long batchCount=0;
		long eventCount=0;//output count
		long outputBytes=0;
		public PublisherStat(){
		}
		public PublisherStat(long publisherId, String destWorkerId) {
			super();
			this.publisherId = publisherId;
			this.destWorkerId = destWorkerId;
			this.startTimestampUS = System.nanoTime()/1000;
			this.lastTimestampUS = startTimestampUS;
		}
		public void toStringBuilder(StringBuilder sb, int indent){
			sb.append(StringUtil.getSpaces(indent));
			sb.append("PublisherStat[");
			sb.append("destWorkerId="); sb.append(destWorkerId);
			//sb.append(", startTimestampUS="); sb.append(startTimestampUS);
			//sb.append(", lastTimestampUS="); sb.append(lastTimestampUS);
			sb.append(", batchCount="); sb.append(batchCount);
			sb.append(", eventCount="); sb.append(eventCount);
			sb.append(", serialTimeUS="); sb.append(serialTimeUS);
			sb.append(", outputTimeUS="); sb.append(outputTimeUS);
			sb.append(", outputBytes="); sb.append(outputBytes);
			sb.append("]");
		}
		public long getPublisherId() {
			return publisherId;
		}
		public void setPublisherId(long publisherId) {
			this.publisherId = publisherId;
		}
		public String getDestWorkerId() {
			return destWorkerId;
		}
		public void setDestWorkerId(String destWorkerId) {
			this.destWorkerId = destWorkerId;
		}
		public long getStartTimestampUS() {
			return startTimestampUS;
		}
		public void setStartTimestampUS(long startTimestampUS) {
			this.startTimestampUS = startTimestampUS;
		}
		public long getLastTimestampUS() {
			return lastTimestampUS;
		}
		public void setLastTimestampUS(long lastTimestampUS) {
			this.lastTimestampUS = lastTimestampUS;
		}
		public double getSerialTimeUS() {
			return serialTimeUS;
		}
		public void setSerialTimeUS(double serialTimeUS) {
			this.serialTimeUS = serialTimeUS;
		}
		public double getOutputTimeUS() {
			return outputTimeUS;
		}
		public void setOutputTimeUS(double outputTimeUS) {
			this.outputTimeUS = outputTimeUS;
		}
		public long getBatchCount() {
			return batchCount;
		}
		public void setBatchCount(long batchCount) {
			this.batchCount = batchCount;
		}
		public long getEventCount() {
			return eventCount;
		}
		public void setEventCount(long eventCount) {
			this.eventCount = eventCount;
		}
		public long getOutputBytes() {
			return outputBytes;
		}
		public void setOutputBytes(long outputBytes) {
			this.outputBytes = outputBytes;
		}
		
	}
//	public InstanceStat(long instanceId, String workerId, String uniqueName, long outputIntervalUS){
//		this.instanceId = instanceId;
//		//this.processorId = processorId;
//		this.workerId = workerId;
//		this.uniqueName = uniqueName;
//		this.batchCount = 0;
//		this.eventCount = 0;
//		this.outputIntervalUS = outputIntervalUS;
//		this.startTimestampUS = System.nanoTime()/1000;
//		this.lastTimestampUS = startTimestampUS;
//		//this.subStats = new SubscriberStat[subscriberCount];
//		this.pubStats = new PublisherStat[0];
//		//this.procInspector.addProcessor(processorId, subscriberIds, conditionCount)
//	}
	
	public void toStringBuilder(StringBuilder sb){
		toStringBuilder(sb, 0);
	}
	
	public void toStringBuilder(StringBuilder sb, int indent){
		sb.append(StringUtil.getSpaces(indent));
		sb.append("InstanceStat[");
		sb.append("workerId="); sb.append(workerId);
		sb.append(", type="); sb.append(type);
		sb.append(", uniqueName="); sb.append(uniqueName);
		sb.append(", outputIntervalUS="); sb.append(outputIntervalUS);
		//sb.append(", startTimestampUS="); sb.append(startTimestampUS);
		//sb.append(", lastTimestampUS="); sb.append(lastTimestampUS);
		sb.append(", batchCount="); sb.append(batchCount);
		sb.append(", eventCount="); sb.append(eventCount);
		sb.append(", procTimeUS="); sb.append(procTimeUS);
		sb.append("]");
		sb.append("\n");
		for(SubscriberStat subStat: this.subStats){
			subStat.toStringBuilder(sb, indent+4);
			sb.append("\n");
		}
		for(PublisherStat pubStat: this.pubStats){
			pubStat.toStringBuilder(sb, indent+4);
			sb.append("\n");
		}
	}
	
	public void addPublisherStat(long publisherId, String destWorkerId){
		PublisherStat[] newPubStats=new PublisherStat[pubStats.length+1];
		pubLock.writeLock().lock();
		System.arraycopy(pubStats, 0, newPubStats, 0, pubStats.length);
		PublisherStat pubStat=new PublisherStat(publisherId, destWorkerId);
		newPubStats[pubStats.length]=pubStat;
		pubStats=newPubStats;
		pubStatMap.put(publisherId, pubStat);
		pubLock.writeLock().unlock();
		pubInspector.addPublisher(publisherId);
	}
	
	public long durationUS(){
		return lastTimestampUS-startTimestampUS;
	}
	
	public double getOutputRateSec(){
		return (double)eventCount*1e6/(double)durationUS();
	}
	
	public void updateSubscriberStat(long subscriberId, long deltaSubEventCount){
		long timestampUS=System.nanoTime()/1000;
		subInspector.updateSubscriberStat(subscriberId, deltaSubEventCount, timestampUS);
		for(SubscriberStat subStat: subStats){
			if(subStat.subscriberId==subscriberId){
				subLock.writeLock().lock();
				subStat.batchCount++;
				subStat.eventCount+=deltaSubEventCount;
				subStat.lastTimestampUS=timestampUS;
				subLock.writeLock().unlock();
				break;
			}
		}
	}
	
	public void updateProcessorProcessingTime(
			//long deltaProcOutputEventCount,
			double deltaProcTimeUS,
			long subscriberId,
			long subEventCount){
		procInspector.updateProcessStat(processorId, //deltaProcOutputEventCount, 
				deltaProcTimeUS, subscriberId, subEventCount);
		procLock.writeLock().lock();
		this.procTimeUS+=deltaProcTimeUS;
		this.lastTimestampUS=System.nanoTime()/1000;
		procLock.writeLock().unlock();
	}
	
	public void updateProcessorOutputCount(long deltaOutEventCount){
		procLock.writeLock().lock();
		this.batchCount++;
		this.eventCount+=deltaOutEventCount;
		this.lastTimestampUS=System.nanoTime()/1000;
		procLock.writeLock().unlock();
	}
	
	public void updatePublisherStat(long publisherId, long deltaOuputEventCount, 
			double deltaSerialTimeUS, double deltaOutputTimeUS, long deltaOuputBytes){
		pubInspector.updatePublishStat(publisherId, deltaOuputEventCount, 
				deltaSerialTimeUS, deltaOutputTimeUS, deltaOuputBytes);
		pubLock.writeLock().lock();
		//TODO: optimize
		PublisherStat pubStat=pubStatMap.get(publisherId);
		if(pubStat!=null){
//		for(PublisherStat pubStat: pubStats){
//			if(pubStat.publisherId==publisherId){
				pubStat.serialTimeUS += deltaSerialTimeUS;
				pubStat.batchCount++;
				pubStat.eventCount += deltaOuputEventCount;
				pubStat.outputTimeUS += deltaOutputTimeUS;
				pubStat.outputBytes += deltaOuputBytes;
				pubStat.lastTimestampUS = System.nanoTime()/1000;
//				break;
//			}
		}
		pubLock.writeLock().unlock();
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUniqueName() {
		return uniqueName;
	}

	public void setUniqueName(String uniqueName) {
		this.uniqueName = uniqueName;
	}

	public long getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(long instanceId) {
		this.instanceId = instanceId;
	}

	public long getProcessorId() {
		return processorId;
	}

	public void setProcessorId(long processorId) {
		this.processorId = processorId;
	}

	public long getStartTimestampUS() {
		return startTimestampUS;
	}

	public void setStartTimestampUS(long startTimestampUS) {
		this.startTimestampUS = startTimestampUS;
	}

	public long getLastTimestampUS() {
		return lastTimestampUS;
	}

	public void setLastTimestampUS(long lastTimestampUS) {
		this.lastTimestampUS = lastTimestampUS;
	}

	public long getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(long batchCount) {
		this.batchCount = batchCount;
	}

	public long getEventCount() {
		return eventCount;
	}

	public void setEventCount(long eventCount) {
		this.eventCount = eventCount;
	}

	public double getProcTimeUS() {
		return procTimeUS;
	}

	public void setProcTimeUS(double procTimeUS) {
		this.procTimeUS = procTimeUS;
	}

	public double getOutputIntervalUS() {
		return outputIntervalUS;
	}

	public void setOutputIntervalUS(double outputIntervalUS) {
		this.outputIntervalUS = outputIntervalUS;
	}

	public SubscriberStat[] getSubStats() {
		return subStats;
	}

	public void setSubStats(SubscriberStat[] subStats) {
		this.subStats = subStats;
	}

	public PublisherStat[] getPubStats() {
		return pubStats;
	}

	public void setPubStats(PublisherStat[] pubStats) {
		this.pubStats = pubStats;
	}

	public SubscriberInspector getSubInspector() {
		return subInspector;
	}

	public void setSubInspector(SubscriberInspector subInspector) {
		this.subInspector = subInspector;
	}

	public ProcessorInspector getProcInspector() {
		return procInspector;
	}

	public void setProcInspector(ProcessorInspector procInspector) {
		this.procInspector = procInspector;
	}

	public PublisherInspector getPubInspector() {
		return pubInspector;
	}

	public void setPubInspector(PublisherInspector pubInspector) {
		this.pubInspector = pubInspector;
	}

	public ReentrantReadWriteLock getSubLock() {
		return subLock;
	}

	public void setSubLock(ReentrantReadWriteLock subLock) {
		this.subLock = subLock;
	}

	public ReentrantReadWriteLock getProcLock() {
		return procLock;
	}

	public void setProcLock(ReentrantReadWriteLock procLock) {
		this.procLock = procLock;
	}

	public ReentrantReadWriteLock getPubLock() {
		return pubLock;
	}

	public void setPubLock(ReentrantReadWriteLock pubLock) {
		this.pubLock = pubLock;
	}
	
}
