package combine.esper.core.cost;

public class WorkerStat {
	public String id;
	public long memUsed=0;//bytes
	public long memFree=0;//bytes	
	public double cpuUsage=0;
	public int cpuCoreCount=0;
	public long cpuHZ=0;
	public int procThreadCount=0;
	public int instanceCount=0;
	public long timestampMS=0;
	
	public WorkerStat() {
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
	public int getInstanceCount() {
		return instanceCount;
	}
	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}
	public long getTimestampMS() {
		return timestampMS;
	}
	public void setTimestampMS(long timestampMS) {
		this.timestampMS = timestampMS;
	}
}
