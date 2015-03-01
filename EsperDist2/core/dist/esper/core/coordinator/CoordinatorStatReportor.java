package dist.esper.core.coordinator;

import java.util.ArrayList;
import java.util.List;

import dist.esper.core.comm.Link;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.GlobalStat;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

public class CoordinatorStatReportor implements Runnable{
	static Logger2 log=Logger2.getLogger(CoordinatorStatReportor.class);
	List<Link> monLinkList=new ArrayList<Link>();
	GlobalStat gs=new GlobalStat();
	
	Coordinator coordinator;
	public CoordinatorStatReportor(Coordinator coordinator){
		this.coordinator = coordinator;
	}

	@Override
	public void run(){
		long timeMS=4000;
		while(true){
			ThreadUtil.sleep(timeMS);
			gs.setEventMap(ServiceManager.getDefaultInstance().getEventRegistry().getEventMap());
			gs.setWorkerIdMap(ServiceManager.getDefaultInstance().getWorkerIdMap());
			gs.setRawStreamStatMap(coordinator.costEval.rawStats.getRawStreamStatMap());
			gs.setContainerNameMap(coordinator.costEval.containerNameMap);
			gs.setContainerStatMap(coordinator.costEval.containerStatMap);
			gs.setProcWorkerStatMap(coordinator.costEval.procWorkerStatMap);
			gs.setGateWorkerStatMap(coordinator.costEval.gateWorkerStatMap);
			gs.setContainerTreeMap(coordinator.containerTreeMap);
			gs.setContainerIdMap(coordinator.containerIdMap);
			monLinkList.clear();
			monLinkList.addAll(coordinator.monitorLinkMap.values());
			for(Link link: monLinkList){
				if(link.isConnected()){
					int bytes=link.send(gs);
					log.debug("*** %s send GlobalStat (%d bytes) to %s\n", coordinator.id, bytes ,link.getTargetId().toString());
				}
			}
		}
	}
}
