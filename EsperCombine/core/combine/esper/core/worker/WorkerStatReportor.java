package combine.esper.core.worker;

import combine.esper.core.cost.WorkerStat;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

class WorkerStatReportor implements Runnable{
	static Logger2 log=Logger2.getLogger(WorkerStatReportor.class);
	public long sendIntervalNS;
	public long updateStatIntervalMS;
	
	Worker worker;
	
	public WorkerStatReportor(Worker worker){
		this(worker, 4000, 460);
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
				//log.info("Worker %s send WorkerStat", worker.id);
				lastSendTimestampNS = currentTimestampNS;
			}
		}
	}
}
