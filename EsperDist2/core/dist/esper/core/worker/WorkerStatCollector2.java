package dist.esper.core.worker;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hyperic.sigar.*;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.comm.rawsocket.async.AsyncRawSocketLinkManager;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.cost.WorkerStat;
import dist.esper.core.flow.container.DerivedStreamContainer;
import dist.esper.core.flow.container.FilterDelayedStreamContainer;
import dist.esper.core.flow.container.FilterStreamContainer;
import dist.esper.core.flow.container.JoinDelayedStreamContainer;
import dist.esper.core.flow.container.JoinStreamContainer;
import dist.esper.core.flow.container.PatternStreamContainer;
import dist.esper.core.flow.container.RootStreamContainer;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.pubsub.Instance;
import dist.esper.core.worker.pubsub.ProcessingScheduler2;
import dist.esper.core.worker.pubsub.Publisher;
import dist.esper.core.worker.pubsub.PublishingScheduler2;
import dist.esper.core.worker.pubsub.Subscriber;
import dist.esper.util.CollectionUtils;
import dist.esper.util.CyclicQueue;

public class WorkerStatCollector2 {
	Sigar sigar;
	volatile WorkerStat workerStat;
	ProcessingScheduler2 procScheduler;
	PublishingScheduler2 pubScheduler;
	SubscriberInspector subInspector;
	ProcessorInspector procInspector;
	PublisherInspector pubInspector;
	AsyncLinkManagerInspector asynclinkMgnInspector=null;
	
	//ReentrantReadWriteLock lock=new ReentrantReadWriteLock();
	ReentrantLock lock=new ReentrantLock();
	
	public WorkerStatCollector2(Worker worker, LinkManager linkManager, 
			ProcessingScheduler2 procScheduler, PublishingScheduler2 pubScheduler){
		this.procScheduler = procScheduler;
		this.pubScheduler = pubScheduler;
		workerStat=new WorkerStat(worker.id);
		workerStat.isGateway=ServiceManager.getInstance(worker.id).getMyWorkerId().isGateway();
		subInspector=new SubscriberInspector(ServiceManager.getOutputIntervalUS());
		procInspector=new ProcessorInspector(subInspector);
		pubInspector=new PublisherInspector(ServiceManager.getOutputIntervalUS());
		if(linkManager instanceof AsyncRawSocketLinkManager){
			asynclinkMgnInspector=new AsyncLinkManagerInspector(ServiceManager.getOutputIntervalUS());
			((AsyncRawSocketLinkManager)linkManager).setStatRecorder(asynclinkMgnInspector);
		}
		sigar=new Sigar();
		initWorkerStat();
	}
	
	public void setGateway(boolean isGateway){
		workerStat.isGateway=isGateway;
	}
	
	public ProcessingScheduler2 getProcessingScheduler() {
		return procScheduler;
	}

	public PublishingScheduler2 getPublishingScheduler() {
		return pubScheduler;
	}

	public SubscriberInspector getSubscriberInspector() {
		return subInspector;
	}

	public ProcessorInspector getProcessorInspector() {
		return procInspector;
	}

	public PublisherInspector getPublisherInspector() {
		return pubInspector;
	}

	private void initWorkerStat(){
		try {
			workerStat.procThreadCount=procScheduler.numThreads;
			workerStat.pubThreadCount=pubScheduler.numThreads;
			CpuInfo[] ci=sigar.getCpuInfoList();
			workerStat.cpuCoreCount=ci.length;
			workerStat.cpuHZ=1000000L*ci[0].getMhz();
		}
		catch (SigarException e) {			
			e.printStackTrace();
		}
	}
	
	public void updateWorkerStat(Worker worker){
		this.lock.lock();
		workerStat.reset(worker.insMap.size(), worker.rawSampler.size());
		int i=0;
		for(Instance ins: worker.insMap.values()){
			workerStat.insStats[i]=ins.getInstanceStat();
			updateStreamConatinerCounter(ins.getStreamContainer());
			for(Subscriber sub: ins.getSubList()){
				if(sub.isLocalSubscriber()) workerStat.localSubscriberCount++;
				else workerStat.remoteSubscriberCount++;
			}
			for(Publisher pub: ins.getPubList()){
				if(pub.isLocalPublisher())	workerStat.localPublisherCount++;
				else workerStat.remotePublisherCount++;
			}
			i++;
		}
		i=0;
		for(RawStreamStat rawStat: worker.rawSampler.getAll()){
			workerStat.rawStats[i]=rawStat;
			i++;
		}
		this.lock.unlock();
	}
	
	public void updateStreamConatinerCounter(DerivedStreamContainer psc){
		if(psc instanceof RootStreamContainer)
			workerStat.rootCount++;		
		else if(psc instanceof JoinDelayedStreamContainer)
			workerStat.joinDelayedCount++;		
		else if(psc instanceof JoinStreamContainer)
			workerStat.joinCount++;		
		else if(psc instanceof FilterDelayedStreamContainer)
			workerStat.filterDelayedCount++;		
		else if(psc instanceof FilterStreamContainer)
			workerStat.filterCount++;
		else if(psc instanceof PatternStreamContainer)
			workerStat.patternCount++;
	}	
	
	public WorkerStat getCurrentWorkerStat(){
		try {
			this.lock.lock();
			workerStat.currentProcQueueSize=procScheduler.getQueueSize();
			workerStat.currentPubQueueSize=pubScheduler.getQueueSize();			
			CpuPerc cpp=sigar.getCpuPerc();
			Mem mem=sigar.getMem();			
			workerStat.cpuUsage=cpp.getCombined();
			workerStat.memUsed=mem.getActualUsed();
			workerStat.memFree=mem.getActualFree();
			workerStat.timestampMS=System.currentTimeMillis();
			
			if(workerStat.id.equals("worker2")){
				System.out.print("");
			}
			procInspector.lock();
			procInspector.computeConditionProcessingTime();
			workerStat.filterCondProcTimeUS=procInspector.filterCondProcTimeUS;
			workerStat.joinCondProcTimeUS=procInspector.joinCondProcTimeUS;
			procInspector.unlock();
			
			if(asynclinkMgnInspector!=null){
				asynclinkMgnInspector.computeSendParameters();
				workerStat.sendBaseTimeUS=asynclinkMgnInspector.sendBaseTimeUS;
				workerStat.sendByteRateUS=asynclinkMgnInspector.sendByteRateUS;
				workerStat.bwUsageUS=asynclinkMgnInspector.computeBandwidthUsage();
			}
			else{
				pubInspector.lock();
				pubInspector.computeSendParameters();
				workerStat.sendBaseTimeUS=pubInspector.sendBaseTimeUS;
				workerStat.sendByteRateUS=pubInspector.sendByteRateUS;
				workerStat.bwUsageUS=pubInspector.computeBandwidthUsage();
				pubInspector.unlock();
			}
			
			this.lock.unlock();
		}
		catch (SigarException e) {
			e.printStackTrace();
		}
		return workerStat;
	}
	
	abstract static class Inspector{
		ReentrantReadWriteLock inspectorLock=new ReentrantReadWriteLock();
		public int QUEUE_SIZE=20;
		public void lock(){
			inspectorLock.writeLock().lock();
		}
		
		public void unlock(){
			inspectorLock.writeLock().unlock();
		}
	}

	public static class SubscriberInspector extends Inspector{
		Map<Long, SubscribeStatQueue> subStatQueueMap=new ConcurrentHashMap<Long, SubscribeStatQueue>();
		Map<Long, Long> subPairMap=new ConcurrentHashMap<Long,Long>();
		long outputIntervalUS;
		
		public SubscriberInspector(long outputIntervalUS) {
			super();
			this.outputIntervalUS = outputIntervalUS;
		}

		public void addSubscriber(long subscriberId, long windowTimeUS){
			if(!subStatQueueMap.containsKey(subscriberId)){
				SubscribeStatQueue subStatQueue=new SubscribeStatQueue(subscriberId, windowTimeUS, outputIntervalUS, QUEUE_SIZE);
				subStatQueueMap.put(subscriberId, subStatQueue);
			}
		}
		
		public void addSubscriberJoinPair(long subscriberId1, long subscriberId2){
			subPairMap.put(subscriberId1, subscriberId2);
			subPairMap.put(subscriberId2, subscriberId1);
		}
		
		public void updateSubscriberStat(long subscriberId, long subEventCount, long timestampUS){
			SubscribeStatQueue subStatQueue=subStatQueueMap.get(subscriberId);
			subStatQueue.lock();
			subStatQueue.alloc().assign(subscriberId, subEventCount, timestampUS);
			subStatQueue.unlock();
		}
		
		public double getPartnerCountWithinWindow(long subscriberId){
			Long partnerSubId=subPairMap.get(subscriberId);
			assert(partnerSubId!=null);
			SubscribeStatQueue subStatQueue=subStatQueueMap.get(partnerSubId);
			assert(subStatQueue!=null);
			return subStatQueue.getCountWithinWindow();
		}
		
		public static class SubscribeStatQueue extends CyclicQueue<SubscribeStat>{
			long subscriberId;
			long windowTimeUS;//0 for filter
			long outputIntervalUS;
			public SubscribeStatQueue(long subscriberId, long windowTimeUS, long outputIntervalUS, int capacity) {
				super(SubscribeStat.class, capacity);
				this.subscriberId = subscriberId;
				this.windowTimeUS = windowTimeUS;
				this.outputIntervalUS = outputIntervalUS;
			}
			public double getCountWithinWindow(){
				long totalCount=0;
				long totalTimeUS=0;
				this.lock();
				if(this.size()>1){
					totalTimeUS=this.get(endSeq-1).timestampUS - this.get(beginSeq).timestampUS;
					totalTimeUS += this.get(endSeq-1).timestampUS - this.get(endSeq-2).timestampUS;//ATT
					for(int i=beginSeq; i<endSeq; i++){
						totalCount+=this.get(i).eventCount;
					}
				}
				else if(this.size()==1){
					totalTimeUS=outputIntervalUS/2;//FIXME
				}
				this.unlock();
				if(totalTimeUS>0){
//					System.out.format("SubscribeStatQueue.size()=%d, windowTimeUS=%d, totalCount=%d, totalTimeUS=%d\n",
//							this.size(), windowTimeUS, totalCount, totalTimeUS);
					return windowTimeUS*totalCount/totalTimeUS;
				}
				else{
					return 0.0;
				}
			}
		}
		public static class SubscribeStat{
			long subscriberId;
			long eventCount;
			long timestampUS;
			public SubscribeStat() {
				super();
			}
			public void assign(long subscriberId, long eventCount,long timestampUS) {
				this.subscriberId = subscriberId;
				this.eventCount = eventCount;
				this.timestampUS = timestampUS;
			}
		}
	}
	
	public static class ProcessorInspector extends Inspector{
		double filterCondProcTimeUS=1.0d;
		double joinCondProcTimeUS=5.0d;
		SubscriberInspector subInspector;
		Map<Long, ProcessStatQueue> procStatQueueMap=new ConcurrentHashMap<Long, ProcessStatQueue>();
		
		public ProcessorInspector(SubscriberInspector subInspector) {
			super();
			this.subInspector = subInspector;
		}
		public void addProcessor(long processorId, long[] subscriberIds, int conditionCount){
			if(!procStatQueueMap.containsKey(processorId)){
				ProcessStatQueue procStatQueue=new ProcessStatQueue(processorId, subscriberIds, conditionCount, QUEUE_SIZE);
				procStatQueueMap.put(processorId, procStatQueue);
			}
		}
		public void updateProcessStat(long processorId, //long procOutputEventCount, 
				double procTimeUS, long subscriberId, long subEventCount){			
			ProcessStatQueue procStatQueue=procStatQueueMap.get(processorId);
			procStatQueue.lock();
			if(procStatQueue.isJoin()){
				double partnerSubEventCount=subInspector.getPartnerCountWithinWindow(subscriberId);
				double procJoinCount=partnerSubEventCount*subEventCount;
//				System.out.format("add to ProcessStatQueue(%d): subEventCount(%d)=%d, partnerSubEventCount=%.1f\n", 
//						processorId, subscriberId, subEventCount, partnerSubEventCount);
				procStatQueue.alloc().assign(processorId, procJoinCount, procTimeUS);
			}
			else{//filter
				procStatQueue.alloc().assign(processorId, subEventCount, procTimeUS);
			}
			procStatQueue.unlock();
		}
		
		public void computeConditionProcessingTime(){
			double totalJoinCount=0.0;
			double totalJoinTimeUSPerCond=0.0;
			double totalFilterCount=0.0;
			double totalFilterTimeUSPerCond=0.0;
			List<ProcessStatQueue> psqList=CollectionUtils.shallowClone(this.procStatQueueMap.values());
			for(ProcessStatQueue psq: psqList){
				if(psq.conditionCount>0){
					psq.lock();
					if(psq.isJoin()){
						for(int i=psq.beginSeq; i<psq.endSeq; i++){
							ProcessStat ps=psq.get(i);
							totalJoinCount+=ps.filteredOrJoinedCount;
							totalJoinTimeUSPerCond+=ps.procTimeUS/psq.conditionCount;
						}
					}
					else{
						for(int i=psq.beginSeq; i<psq.endSeq; i++){
							ProcessStat ps=psq.get(i);
							totalFilterCount+=ps.filteredOrJoinedCount;
							totalFilterTimeUSPerCond+=ps.procTimeUS/psq.conditionCount;
						}
					}
					psq.unlock();
				}
			}
			if(totalFilterCount>0){
				filterCondProcTimeUS=totalFilterTimeUSPerCond/totalFilterCount;
			}
			if(totalJoinCount>0){
				joinCondProcTimeUS=totalJoinTimeUSPerCond/totalJoinCount;
			}
		}
		public static class ProcessStatQueue extends CyclicQueue<ProcessStat>{
			long processorId;
			long[] subscriberIds;
			int conditionCount;
			public ProcessStatQueue(long processorId, long[] subscriberIds, int conditionCount, int capacity) {
				super(ProcessStat.class, capacity);
				this.processorId = processorId;
				this.subscriberIds = subscriberIds;
				this.conditionCount = conditionCount;
			}
			public boolean isJoin(){
				return subscriberIds.length>=2;
			}
			@Override
			public String toString(){
				return String.format("ProcessStatQueue[%d,%s,%d,%s]", processorId, Arrays.toString(subscriberIds), conditionCount, isLocked());
			}
		}
		public static class ProcessStat{
			long processorId;
			double filteredOrJoinedCount;
			double procTimeUS;
			public ProcessStat(){
				super();
			}
			public void assign(long processorId, double procFilterOrJoinCount, double procTimeUS) {
				this.processorId = processorId;
				this.filteredOrJoinedCount = procFilterOrJoinCount;
				this.procTimeUS = procTimeUS;
			}
		}
	}
	
	public static class PublisherInspector extends Inspector{
		long outputIntervalUS=0;
		double sendBaseTimeUS=0;
		double sendByteRateUS=0;
		Map<Long, PublishStatQueue> pubStatQueueMap=new ConcurrentHashMap<Long, PublishStatQueue>();
		
		public PublisherInspector(long outputIntervalUS) {
			super();
			this.outputIntervalUS = outputIntervalUS;
		}
		public void addPublisher(long publisherId){
			this.lock();
			if(!pubStatQueueMap.containsKey(publisherId)){
				PublishStatQueue pubStatQueue=new PublishStatQueue(publisherId, QUEUE_SIZE);
				pubStatQueueMap.put(publisherId, pubStatQueue);
			}
			this.unlock();
		}
		public void updatePublishStat(long publisherId, long eventCount,
				double serialTimeUS, double sendTimeUS, long sendBytes){
			PublishStatQueue queue=pubStatQueueMap.get(publisherId);
			queue.lock();
			queue.alloc().assign(publisherId, eventCount, serialTimeUS, sendTimeUS, sendBytes);
			queue.unlock();
		}
		
		public double computeBandwidthUsage(){
			double totalSendTimeUS=0.0;
			List<PublishStatQueue> queueList=CollectionUtils.shallowClone(pubStatQueueMap.values());
			for(PublishStatQueue queue: queueList){
				queue.lock();
				double totalTime=0.0;
				if(queue.size()>0){
					int beginSeq=queue.beginSeq;
					int endSeq=queue.endSeq;
					for(int i=beginSeq; i<endSeq; i++){
						totalTime += queue.get(i).sendTimeUS;
					}
					totalTime=totalTime/queue.size();
				}
				else{
					totalTime=0.0;
				}
				queue.unlock();
				totalSendTimeUS+=totalTime;
			}
			return totalSendTimeUS*1e6/this.outputIntervalUS;
		}
		
		public void computeSendParameters(){
			//TODO
			sendBaseTimeUS=50.0;
			sendByteRateUS=5.0;
		}
		
		public static class PublishStatQueue extends CyclicQueue<PublishStat>{
			long publisherId;
			public PublishStatQueue(long publisherId, int capacity) {
				super(PublishStat.class, capacity);
				this.publisherId = publisherId;
			}
		}
		
		public static class PublishStat{
			long publisherId;
			long eventCount;
			double serialTimeUS;
			double sendTimeUS;
			long sendBytes;
			public PublishStat() {
				super();
			}
			public void assign(long publisherId, long eventCount,
					double serialTimeUS, double sendTimeUS, long sendBytes) {
				this.publisherId = publisherId;
				this.eventCount = eventCount;
				this.serialTimeUS = serialTimeUS;
				this.sendTimeUS = sendTimeUS;
				this.sendBytes = sendBytes;
			}
		}
	}
	
	public static class AsyncLinkManagerInspector 
		extends Inspector implements AsyncRawSocketLinkManager.IStatRecorder{
		LinkManagerStatQueue linkMngStatQueue;
		LinkManagerStat curLinkMngStat;
		long outputIntervalUS;
		double sendBaseTimeUS=0;
		double sendByteRateUS=0;
		
		public AsyncLinkManagerInspector(long outputIntervalUS) {
			super();
			this.outputIntervalUS = outputIntervalUS;
			linkMngStatQueue=new LinkManagerStatQueue(QUEUE_SIZE);
		}

		@Override
		public void beginRound(int linkCount) {
			//linkMngStatQueue.lock();
			curLinkMngStat=linkMngStatQueue.alloc();
			curLinkMngStat.beginAssign(linkCount);
		}

		@Override
		public void record(long linkId, int sendBytes, long sendTimeUS) {
			curLinkMngStat.assign(linkId, sendBytes, sendTimeUS);
		}

		@Override
		public void endRound(long roundTimeUS) {
			if(curLinkMngStat==null){
				System.out.print("");
			}
			curLinkMngStat.endAssign(roundTimeUS);
			
			//linkMngStatQueue.unlock();
			curLinkMngStat=null;
		}
		
		public void computeSendParameters(){
			//TODO
			sendBaseTimeUS=50.0;
			sendByteRateUS=5.0;
		}
		
		public double computeBandwidthUsage(){
			int beginSeq=linkMngStatQueue.beginSeq;
			int endSeq=linkMngStatQueue.endSeq;
			int count=0;
			long roundTimeUS;
			long totalRoundTimeUS=0;
			for(int i=beginSeq; i<endSeq; i++){
				roundTimeUS=linkMngStatQueue.get(i).roundTimeUS;
				if(roundTimeUS>0){
					count++;
					totalRoundTimeUS+=roundTimeUS;
				}
			}
			if(count>0){
				return (totalRoundTimeUS*1e6)/(double)(count*outputIntervalUS);
			}
			return 0.0;
		}
		
		public static class LinkManagerStatQueue extends CyclicQueue<LinkManagerStat>{
			public LinkManagerStatQueue(int capacity) {
				super(LinkManagerStat.class, capacity);
			}
		}
		
		public static class LinkManagerStat{
			LinkStat[] linkStats;
			long roundTimeUS=-1;
			int actualCount=0;
			public void beginAssign(int linkCount){
				if(linkStats==null || linkStats.length<linkCount){
					linkStats=new LinkStat[linkCount];
					for(int i=0;i<linkStats.length;i++){
						linkStats[i]=new LinkStat();
					}
				}
				actualCount=0;
				roundTimeUS=-1;
			}
			public void endAssign(long roundTimeUS){
				this.roundTimeUS=roundTimeUS;
			}
			public void assign(long linkId, int sendBytes, long sendTimeUS){
				linkStats[actualCount].assign(linkId, sendBytes, sendTimeUS);
				actualCount++;
			}
		}
		
		public static class LinkStat{
			long linkId;
			int sendBytes;
			long sendTimeUS;
			public LinkStat(){
				super();
			}
			public void assign(long linkId, int sendBytes, long sendTimeUS){
				this.linkId=linkId;
				this.sendBytes=sendBytes;
				this.sendTimeUS=sendTimeUS;
			}
		}
	}
}
