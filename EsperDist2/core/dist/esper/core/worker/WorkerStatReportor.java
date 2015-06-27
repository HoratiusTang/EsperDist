package dist.esper.core.worker;

import com.esotericsoftware.minlog.Log;

import dist.esper.core.comm.Link;
import dist.esper.core.cost.WorkerStat;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

class WorkerStatReportor implements Runnable{
	static Logger2 log=Logger2.getLogger(Link.class);
	public long sendIntervalNS;
	public long updateStatIntervalMS;
	
	Worker worker;
	
	public WorkerStatReportor(Worker worker){
		this(worker, 3000, 3000);
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
			
			//log.debug("WorkerStatReportor wake up");
			long currentTimestampNS=System.nanoTime();
			if( currentTimestampNS - lastSendTimestampNS >= sendIntervalNS){				
				WorkerStat ws=worker.workerStatCollector.getCurrentWorkerStat();				
				worker.coordLink.send(ws);
				log.debug("WorkerStatReportor send WorkerStat to Coordinator: insStats.count=%d, rawStats.count=%d", 
						ws.insStats.length, ws.rawStats.length);
				lastSendTimestampNS = currentTimestampNS;
			}
		}
	}
}
