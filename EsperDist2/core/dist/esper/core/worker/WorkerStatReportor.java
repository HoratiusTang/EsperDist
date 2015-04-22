package dist.esper.core.worker;

import dist.esper.core.cost.WorkerStat;
import dist.esper.util.ThreadUtil;

class WorkerStatReportor implements Runnable{
	public long sendIntervalNS;
	public long updateStatIntervalMS;
	
	Worker worker;
	
	public WorkerStatReportor(Worker worker){
		this(worker, 3000, 600);
	}
	public WorkerStatReportor(Worker worker, long sendIntervalMS, long updateStatIntervalMS) {
		super();
		this.worker = worker;
		this.sendIntervalNS = sendIntervalMS * 1000000;
		this.updateStatIntervalMS = updateStatIntervalMS;
	}
	
	@Override
	public void run(){
		long lastSendTimestampNS=System.nanoTime();
		while(true){
			ThreadUtil.sleep(updateStatIntervalMS);
			//worker.workerStatCollector.updateCPUAndMem();
			
			long currentTimestampNS=System.nanoTime();
			if( currentTimestampNS - lastSendTimestampNS > sendIntervalNS){				
				WorkerStat ws=worker.workerStatCollector.getCurrentWorkerStat();
				worker.coordLink.send(ws);
				lastSendTimestampNS = currentTimestampNS;
			}
		}
	}
}
