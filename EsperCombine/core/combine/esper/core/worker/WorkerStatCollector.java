package combine.esper.core.worker;

import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import combine.esper.core.cost.WorkerStat;
import dist.esper.core.worker.pubsub.ProcessingScheduler2;

public class WorkerStatCollector {
	Sigar sigar;
	volatile WorkerStat workerStat;
	Worker worker;
	ProcessingScheduler2 procScheduler;
	
	public WorkerStatCollector(Worker worker,
			ProcessingScheduler2 procScheduler){
		this.worker = worker;
		this.procScheduler = procScheduler;
		workerStat=new WorkerStat(worker.id);
		sigar=new Sigar();
		initWorkerStat();
	}
	private void initWorkerStat(){
		try {
			workerStat.procThreadCount=procScheduler.numThreads;
			CpuInfo[] ci=sigar.getCpuInfoList();
			workerStat.cpuCoreCount=ci.length;
			workerStat.cpuHZ=1000000L*ci[0].getMhz();
		}
		catch (SigarException e) {			
			e.printStackTrace();
		}
	}
	
	public WorkerStat getCurrentWorkerStat(){
		try {
			CpuPerc cpp=sigar.getCpuPerc();
			Mem mem=sigar.getMem();			
			workerStat.cpuUsage=cpp.getCombined();
			workerStat.memUsed=mem.getActualUsed();
			workerStat.memFree=mem.getActualFree();
			workerStat.timestampMS=System.currentTimeMillis();
			workerStat.instanceCount=worker.insMap.size();
			return workerStat;
		}
		catch (SigarException e) {
			e.printStackTrace();
			return workerStat;
		}
	}
}
