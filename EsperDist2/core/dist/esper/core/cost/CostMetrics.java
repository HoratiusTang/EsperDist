package dist.esper.core.cost;

import java.util.*;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import dist.esper.core.util.NumberFormatter;
import static dist.esper.core.util.NumberFormatter.format;

public class CostMetrics {
	public static final StandardDeviation var=new StandardDeviation();
	public int index;
	public double deltaMemoryBytes=0;
	public double deltaCPUTimeUS=0;
	public double deltaOutputTimeUS=0;
	
	public double memoryBytesVariance=0;
	public double cpuTimeUSVariance=0;
	public double outputTimeUSVariance=0;
	
	public int workerCount=0;
	
	public SortedMap<String,WorkerResourceUsage> wruMap=null;
	
	class WorkerResourceUsage{
		public String id;
		public double deltaMemoryBytes=0;
		public double deltaCPUTimeUS=0;
		public double deltaOutputTimeUS=0;
		
		public double memoryBytes;
		public double CPUTimeUS;
		public double outputTimeUS;
		public WorkerResourceUsage(String id, double memoryBytes,
				double cpuTimeUS, double outputTimeUS) {
			super();
			this.id = id;
			this.memoryBytes = memoryBytes;
			CPUTimeUS = cpuTimeUS;
			this.outputTimeUS = outputTimeUS;
		}
		public double getAccumulatedMemoryBytes(){
			return memoryBytes+deltaMemoryBytes;
		}
		public double getAccumulatedCPUTimeUS(){
			return CPUTimeUS+deltaCPUTimeUS;
		}
		public double getAccumulatedOutputTimeUS(){
			return outputTimeUS+deltaOutputTimeUS;
		}
		public boolean isUsed(){
			return this.deltaMemoryBytes>0 || this.deltaCPUTimeUS>0 || this.deltaOutputTimeUS>0; 
		}
		@Override
		public String toString(){
			StringBuilder sb=new StringBuilder();
			sb.append("WorkerResourceUsage("+id+")[");
			sb.append("\n\tdeltaMemoryBytes="); sb.append(format(deltaMemoryBytes));
			sb.append("\n\tdeltaCPUTimeUS="); sb.append(format(deltaCPUTimeUS));
			sb.append("\n\tdeltaOutputTimeUS="); sb.append(format(deltaOutputTimeUS));
			sb.append("\n\tmemoryBytes="); sb.append(format(memoryBytes));
			sb.append("\n\tCPUTimeUS="); sb.append(format(CPUTimeUS));
			sb.append("\n\toutputTimeUS="); sb.append(format(outputTimeUS));
			sb.append("]");
			return sb.toString();
		}
	}
	
	public void addDeltaWorkerResourceUsage(String workerId, 
		double dMemoryBytes, double dCPUTimeUS, double dOutputTimeUS){
		WorkerResourceUsage wru=wruMap.get(workerId);
		wru.deltaMemoryBytes += dMemoryBytes;
		wru.deltaCPUTimeUS += dCPUTimeUS;
		wru.deltaOutputTimeUS += dOutputTimeUS;
	}
	
	public void initWorkerResourceUsageMap(Map<String,WorkerStat> wsMap){
		wruMap=new TreeMap<String,WorkerResourceUsage>();
		for(WorkerStat ws: wsMap.values()){
			double memoryBytes=ws.memUsed;
			double outputTimeUS=ws.bwUsageUS;
			double cpuTimeUS=ws.cpuUsage * ws.cpuCoreCount * 1e6;
			wruMap.put(ws.id, new WorkerResourceUsage(ws.id, memoryBytes, cpuTimeUS, outputTimeUS));
		}
	}
	
	public void computeState(){
		//Variance var=new Variance();
		double[] accMemoryBytes=new double[wruMap.size()];
		double[] accCPUTimeUS=new double[wruMap.size()];
		double[] accOutputTimeUS=new double[wruMap.size()];
		Arrays.fill(accMemoryBytes, 0.0);
		Arrays.fill(accCPUTimeUS, 0.0);
		Arrays.fill(accOutputTimeUS, 0.0);
		
		workerCount=0;
		int i=0;
		for(WorkerResourceUsage ws: wruMap.values()){
			accMemoryBytes[i]=ws.getAccumulatedMemoryBytes();
			accCPUTimeUS[i]=ws.getAccumulatedCPUTimeUS();
			accOutputTimeUS[i]=ws.getAccumulatedOutputTimeUS();
			if(ws.isUsed()){
				workerCount++;
				this.deltaMemoryBytes += ws.deltaMemoryBytes;
				this.deltaCPUTimeUS += ws.deltaCPUTimeUS;
				this.deltaOutputTimeUS += ws.deltaOutputTimeUS;
			}
			i++;
		}
		memoryBytesVariance=var.evaluate(accMemoryBytes);
		cpuTimeUSVariance=var.evaluate(accCPUTimeUS);
		outputTimeUSVariance=var.evaluate(accOutputTimeUS);
	}
	
	public static Comparator<CostMetrics> comparator=new DefaultComparator();
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("CostMetrics(%d)[[",index));
		String dem="";
		for(WorkerResourceUsage wru: wruMap.values()){			
			sb.append(dem+wru.id+":"+wru.isUsed());
			dem=", ";
		}
		sb.append("][");
		sb.append("\n\tdeltaOutputTimeUS="); sb.append(format(deltaOutputTimeUS));
		sb.append(", outputTimeUSVariance="); sb.append(format(outputTimeUSVariance));
		sb.append(", d*sqrt(v)="); sb.append(format(deltaOutputTimeUS*Math.sqrt(outputTimeUSVariance)));
		
		sb.append("\n\tdeltaMemoryBytes="); sb.append(format(deltaMemoryBytes));
		sb.append(", memoryBytesVariance="); sb.append(format(memoryBytesVariance));
		sb.append(", d*sqrt(v)="); sb.append(format(deltaMemoryBytes*Math.sqrt(memoryBytesVariance)));
		
		sb.append("\n\tdeltaCPUTimeUS="); sb.append(format(deltaCPUTimeUS));
		sb.append(", cpuTimeUSVariance="); sb.append(format(cpuTimeUSVariance));
		sb.append(", d*sqrt(v)="); sb.append(format(deltaCPUTimeUS*Math.sqrt(cpuTimeUSVariance)));
		sb.append("]");
		return sb.toString();
	}
	
	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public CostMetrics() {
		super();
	}

	public double getDeltaMemoryBytes() {
		return deltaMemoryBytes;
	}

	public double getDeltaCPUTimeUS() {
		return deltaCPUTimeUS;
	}

	public double getDeltaOutputTimeUS() {
		return deltaOutputTimeUS;
	}
	
	public double addDeltaMemoryBytes(double d) {
		return deltaMemoryBytes+=d;
	}

	public double addDeltaCPUTimeUS(double d) {
		return deltaCPUTimeUS+=d;
	}

	public double addDeltaOutputTimeUS(double d) {
		return deltaOutputTimeUS+=d;
	}
	
	public static class DefaultComparator implements Comparator<CostMetrics>{
//		@Override
//		public int compare(CostMetrics a, CostMetrics b) {
//			if(a.deltaOutputTimeUS!=b.deltaOutputTimeUS){
//				return a.deltaOutputTimeUS>b.deltaOutputTimeUS?1:-1;
//			}
//			else if(a.deltaCPUTimeUS!=b.deltaCPUTimeUS){
//				return a.deltaCPUTimeUS>b.deltaCPUTimeUS?1:-1;
//			}
//			else if(a.deltaMemoryBytes!=b.deltaMemoryBytes){
//				return a.deltaMemoryBytes>b.deltaMemoryBytes?1:-1;
//			}
//			return 0;
//		}
		@Override
		public int compare(CostMetrics a, CostMetrics b) {
			double out1=a.deltaOutputTimeUS * Math.sqrt(a.outputTimeUSVariance);
			double cpu1=a.deltaCPUTimeUS * Math.sqrt(a.cpuTimeUSVariance);
			double mem1=a.deltaMemoryBytes * Math.sqrt(a.memoryBytesVariance);
			
			double out2=b.deltaOutputTimeUS * Math.sqrt(b.outputTimeUSVariance);
			double cpu2=b.deltaCPUTimeUS * Math.sqrt(b.cpuTimeUSVariance);
			double mem2=b.deltaMemoryBytes * Math.sqrt(b.memoryBytesVariance);
			
			double outAvg=(out1+out2)/2;
			double cpuAvg=(cpu1+cpu2)/2;
			double memAvg=(mem1+mem2)/2;
			
			outAvg=(outAvg==0)?Double.MIN_NORMAL:outAvg;
			cpuAvg=(cpuAvg==0)?Double.MIN_NORMAL:cpuAvg;
			memAvg=(memAvg==0)?Double.MIN_NORMAL:memAvg;
			
			double outR1=out1/outAvg;
			double outR2=out2/outAvg;			
			double cpuR1=cpu1/cpuAvg;
			double cpuR2=cpu2/cpuAvg;
			double memR1=mem1/memAvg;
			double memR2=mem2/memAvg;
			
			double v1= outR1 * outR1 + cpuR1 * cpuR1 + memR1 * memR1;
			double v2= outR2 * outR2 + cpuR2 * cpuR2 + memR2 * memR2;
			
			if(v1>v2){
				return 1;
			}
			else if(v1<v2){
				return -1;
			}
			/**
			if(out1!=out2){
				return out1>out2?1:-1;
			}
			else if(cpu1!=cpu2){
				return cpu1>cpu2?1:-1;
			}
			else if(mem1!=mem2){
				return mem1>mem2?1:-1;
			}*/
			return 0;
		}
	}
}
