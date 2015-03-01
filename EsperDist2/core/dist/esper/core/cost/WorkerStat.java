package dist.esper.core.cost;

import java.io.Serializable;

public class WorkerStat implements Serializable{
private static final long serialVersionUID = 6077447393633191724L;
	public static double DEFAULT_SEND_BYTE_RATE_US=5.0;//40 mbps
	public String id;
	public boolean isGateway=false;
	
	public long timestampMS=0;
	
	public long memUsed=0;//bytes
	public long memFree=0;//bytes
	public double bwUsageUS=0;
	public double cpuUsage=0;
	public int cpuCoreCount=0;
	public long cpuHZ=0;
	public int procThreadCount=0;
	public int pubThreadCount=0;//PublishingScheduler.numThreads
	
	//long intervalUS;//us
	
	public int localSubscriberCount=0;
	public int remoteSubscriberCount=0;//sub的总数就是一定时间内期望到达的数量
	public int localPublisherCount=0;
	public int remotePublisherCount=0;
	
	//int inRemoteLinkCount;
	//int outRemoteLinkCount;
	
	public int recvThreadCount=0;//set in kryonet?	
	
	public long totalProcCount=0;
	public long recentProcCount=0;//queue size
	public double recentProcIntervalUS=0;
	public double procTimeUS=0;//ProcessingScheduler, procTimeUS/recentProcIntervalUS+serialTimeUS/recentPubIntervalUS=proc time per s
	public double procDelayTimeUS=0;//ProcessingScheduler
	public long currentProcQueueSize=0;
	
	public long totalPubCount=0;
	public long recentPubCount=0;
	public double recentPubIntervalUS=0;
	public double serialTimeUS=0;//us, in total
	public double pubDelayTimeUS=0;
	public double sendTimeUS=0;//us, in total
	public double sendDelayTimeUS=0;
	public long currentPubQueueSize=0;
	
	public double sendByteRateUS=0;//bytes per us
	public double sendBaseTimeUS=0;//us
	
	public double filterCondProcTimeUS=0;//us
	public double joinCondProcTimeUS=0;
	public int filterCount=0;
	public int filterCompCount=0;
	public int patternCount=0;
	public int patternCompCount=0;
	public int joinCount=0;
	public int joinCompCount=0;
	public int rootCount=0;
	public int rawStatsCount=0;
	public InstanceStat[] insStats=new InstanceStat[0];
	public RawStreamStat[] rawStats=new RawStreamStat[0];
	
	@Override
	public String toString(){
		String basicStr=String.format("WorkerStat[id=%s, isGateway=%s, timestampMS=%d, procThreadCount=%d, pubThreadCount=%d, " +
				"instanceCount=%d, subscriberCount=%d, publisherCount=%d, " +
				"currentProcQueueSize=%d, currentPubQueueSize=%d, " +
				"cpuUsage=%.4f, memUsed=%d, memFree=%d, bwUsageUS=%.2f," +
				"filterCondProcTimeUS=%.2f, joinCondProcTimeUS=%.2f]", 
				id,	
				isGateway,
				timestampMS,
				procThreadCount,
				pubThreadCount,
				getInstanceCount(), 
				getSubscriberCount(), 
				getPublisherCount(),				
				currentProcQueueSize,
				currentPubQueueSize,
				cpuUsage,
				memUsed,
				memFree,
				bwUsageUS,
				filterCondProcTimeUS,
				joinCondProcTimeUS
				);
		StringBuilder sb=new StringBuilder();
		sb.append(basicStr);
		sb.append("\n");
		for(InstanceStat insStat: insStats){
			insStat.toStringBuilder(sb, 4);
		}
		for(RawStreamStat rawStat: rawStats){
			rawStat.toStringBuilder(sb, 4);
			sb.append("\n");
		}
		return sb.toString();
	}
	
//	@Override
//	public String toString(){
//		return String.format("WorkerStat[id=%s, procThreadCount=%d, pubThreadCount=%d, " +
//				"instanceCount=%d, subscriberCount=%d, publisherCount=%d, " +
//				"totalProcCount=%d, recentProcCount=%d, currentProcQueueSize=%d, recentProcIntervalUS=%.0f, procDelayTimeUS=%.0f, procTimeUS=%.0f, " +
//				"totalPubCount=%d, recentPubCount=%d, currentPubQueueSize=%d, recentPubIntervalUS=%.0f, pubDelayTimeUS=%.0f, serialTimeUS=%.0f, sendTimeUS=%.0f]\n", 
//				id,	
//				procThreadCount,
//				pubThreadCount,
//				getInstanceCount(), 
//				getSubscriberCount(), 
//				getPublisherCount(),
//				totalProcCount,
//				recentProcCount,
//				currentProcQueueSize,
//				recentProcIntervalUS,
//				procDelayTimeUS,
//				procTimeUS,
//				totalPubCount,
//				recentPubCount,
//				currentPubQueueSize,
//				recentPubIntervalUS,
//				pubDelayTimeUS,
//				serialTimeUS,
//				sendTimeUS
//				);
//	}
	
	public void reset(int instanceCount, int rawSamplerCount){
		timestampMS=0;
		localSubscriberCount=0;
		remoteSubscriberCount=0;
		localPublisherCount=0;
		remotePublisherCount=0;
		filterCount=0;
		filterCompCount=0;
		patternCount=0;
		patternCompCount=0;
		joinCount=0;
		joinCompCount=0;
		rootCount=0;
		rawStatsCount=rawSamplerCount;
		
		if(insStats==null || insStats.length<instanceCount){
			insStats=new InstanceStat[instanceCount];
		}
		if(rawStats==null || rawStats.length<rawStatsCount){
			rawStats=new RawStreamStat[rawStatsCount];
		}
	}
	public WorkerStat(){
		super();
	}
	
	public WorkerStat(String id) {
		super();
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public long getTimestampMS() {
		return timestampMS;
	}

	public void setTimestampMS(long timestampMS) {
		this.timestampMS = timestampMS;
	}

	public int getSubscriberCount(){
		return this.localSubscriberCount + this.remoteSubscriberCount;
	}
	
	public int getPublisherCount(){
		return this.localPublisherCount + this.remotePublisherCount;
	}
	
	public int getInstanceCount(){
		return filterCount+filterCompCount+patternCount+
				patternCompCount+joinCount+joinCompCount+rootCount;
	}
	
	public double getSendByteRateUS() {
		if(this.sendBaseTimeUS==0.0){
			return DEFAULT_SEND_BYTE_RATE_US;
		}
		return sendByteRateUS;
	}
	public void setSendByteRateUS(double avgSendByteRateUS) {
		this.sendByteRateUS = avgSendByteRateUS;
	}

	public boolean isGateway() {
		return isGateway;
	}

	public void setGateway(boolean isGateway) {
		this.isGateway = isGateway;
	}

	public long getMemUsed() {
		return memUsed;
	}

	public void setMemUsed(long memUsed) {
		this.memUsed = memUsed;
	}

	public long getMemFree() {
		return memFree;
	}

	public void setMemFree(long memFree) {
		this.memFree = memFree;
	}

	public double getBwUsageUS() {
		return bwUsageUS;
	}

	public void setBwUsageUS(double bwUsageUS) {
		this.bwUsageUS = bwUsageUS;
	}

	public double getCpuUsage() {
		return cpuUsage;
	}

	public void setCpuUsage(double cpuUsage) {
		this.cpuUsage = cpuUsage;
	}

	public int getCpuCoreCount() {
		return cpuCoreCount;
	}

	public void setCpuCoreCount(int cpuCoreCount) {
		this.cpuCoreCount = cpuCoreCount;
	}

	public long getCpuHZ() {
		return cpuHZ;
	}

	public void setCpuHZ(long cpuHZ) {
		this.cpuHZ = cpuHZ;
	}

	public int getProcThreadCount() {
		return procThreadCount;
	}

	public void setProcThreadCount(int procThreadCount) {
		this.procThreadCount = procThreadCount;
	}

	public int getPubThreadCount() {
		return pubThreadCount;
	}

	public void setPubThreadCount(int pubThreadCount) {
		this.pubThreadCount = pubThreadCount;
	}

	public int getLocalSubscriberCount() {
		return localSubscriberCount;
	}

	public void setLocalSubscriberCount(int localSubscriberCount) {
		this.localSubscriberCount = localSubscriberCount;
	}

	public int getRemoteSubscriberCount() {
		return remoteSubscriberCount;
	}

	public void setRemoteSubscriberCount(int remoteSubscriberCount) {
		this.remoteSubscriberCount = remoteSubscriberCount;
	}

	public int getLocalPublisherCount() {
		return localPublisherCount;
	}

	public void setLocalPublisherCount(int localPublisherCount) {
		this.localPublisherCount = localPublisherCount;
	}

	public int getRemotePublisherCount() {
		return remotePublisherCount;
	}

	public void setRemotePublisherCount(int remotePublisherCount) {
		this.remotePublisherCount = remotePublisherCount;
	}

	public int getRecvThreadCount() {
		return recvThreadCount;
	}

	public void setRecvThreadCount(int recvThreadCount) {
		this.recvThreadCount = recvThreadCount;
	}

	public long getTotalProcCount() {
		return totalProcCount;
	}

	public void setTotalProcCount(long totalProcCount) {
		this.totalProcCount = totalProcCount;
	}

	public long getRecentProcCount() {
		return recentProcCount;
	}

	public void setRecentProcCount(long recentProcCount) {
		this.recentProcCount = recentProcCount;
	}

	public double getRecentProcIntervalUS() {
		return recentProcIntervalUS;
	}

	public void setRecentProcIntervalUS(double recentProcIntervalUS) {
		this.recentProcIntervalUS = recentProcIntervalUS;
	}

	public double getProcTimeUS() {
		return procTimeUS;
	}

	public void setProcTimeUS(double procTimeUS) {
		this.procTimeUS = procTimeUS;
	}

	public double getProcDelayTimeUS() {
		return procDelayTimeUS;
	}

	public void setProcDelayTimeUS(double procDelayTimeUS) {
		this.procDelayTimeUS = procDelayTimeUS;
	}

	public long getCurrentProcQueueSize() {
		return currentProcQueueSize;
	}

	public void setCurrentProcQueueSize(long currentProcQueueSize) {
		this.currentProcQueueSize = currentProcQueueSize;
	}

	public long getTotalPubCount() {
		return totalPubCount;
	}

	public void setTotalPubCount(long totalPubCount) {
		this.totalPubCount = totalPubCount;
	}

	public long getRecentPubCount() {
		return recentPubCount;
	}

	public void setRecentPubCount(long recentPubCount) {
		this.recentPubCount = recentPubCount;
	}

	public double getRecentPubIntervalUS() {
		return recentPubIntervalUS;
	}

	public void setRecentPubIntervalUS(double recentPubIntervalUS) {
		this.recentPubIntervalUS = recentPubIntervalUS;
	}

	public double getSerialTimeUS() {
		return serialTimeUS;
	}

	public void setSerialTimeUS(double serialTimeUS) {
		this.serialTimeUS = serialTimeUS;
	}

	public double getPubDelayTimeUS() {
		return pubDelayTimeUS;
	}

	public void setPubDelayTimeUS(double pubDelayTimeUS) {
		this.pubDelayTimeUS = pubDelayTimeUS;
	}

	public double getSendTimeUS() {
		return sendTimeUS;
	}

	public void setSendTimeUS(double sendTimeUS) {
		this.sendTimeUS = sendTimeUS;
	}

	public double getSendDelayTimeUS() {
		return sendDelayTimeUS;
	}

	public void setSendDelayTimeUS(double sendDelayTimeUS) {
		this.sendDelayTimeUS = sendDelayTimeUS;
	}

	public long getCurrentPubQueueSize() {
		return currentPubQueueSize;
	}

	public void setCurrentPubQueueSize(long currentPubQueueSize) {
		this.currentPubQueueSize = currentPubQueueSize;
	}

	public double getSendBaseTimeUS() {
		return sendBaseTimeUS;
	}

	public void setSendBaseTimeUS(double sendBaseTimeUS) {
		this.sendBaseTimeUS = sendBaseTimeUS;
	}

	public double getFilterCondProcTimeUS() {
		return filterCondProcTimeUS;
	}

	public void setFilterCondProcTimeUS(double filterCondProcTimeUS) {
		this.filterCondProcTimeUS = filterCondProcTimeUS;
	}

	public double getJoinCondProcTimeUS() {
		return joinCondProcTimeUS;
	}

	public void setJoinCondProcTimeUS(double joinCondProcTimeUS) {
		this.joinCondProcTimeUS = joinCondProcTimeUS;
	}

	public int getFilterCount() {
		return filterCount;
	}

	public void setFilterCount(int filterCount) {
		this.filterCount = filterCount;
	}

	public int getFilterCompCount() {
		return filterCompCount;
	}

	public void setFilterCompCount(int filterCompCount) {
		this.filterCompCount = filterCompCount;
	}

	public int getPatternCount() {
		return patternCount;
	}

	public void setPatternCount(int patternCount) {
		this.patternCount = patternCount;
	}

	public int getPatternCompCount() {
		return patternCompCount;
	}

	public void setPatternCompCount(int patternCompCount) {
		this.patternCompCount = patternCompCount;
	}

	public int getJoinCount() {
		return joinCount;
	}

	public void setJoinCount(int joinCount) {
		this.joinCount = joinCount;
	}

	public int getJoinCompCount() {
		return joinCompCount;
	}

	public void setJoinCompCount(int joinCompCount) {
		this.joinCompCount = joinCompCount;
	}

	public int getRootCount() {
		return rootCount;
	}

	public void setRootCount(int rootCount) {
		this.rootCount = rootCount;
	}

	public int getRawStatsCount() {
		return rawStatsCount;
	}

	public void setRawStatsCount(int rawStatsCount) {
		this.rawStatsCount = rawStatsCount;
	}

	public InstanceStat[] getInsStats() {
		return insStats;
	}

	public void setInsStats(InstanceStat[] insStats) {
		this.insStats = insStats;
	}

	public RawStreamStat[] getRawStats() {
		return rawStats;
	}

	public void setRawStats(RawStreamStat[] rawStats) {
		this.rawStats = rawStats;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
}
