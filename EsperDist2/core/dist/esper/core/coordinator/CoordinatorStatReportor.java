package dist.esper.core.coordinator;

import java.util.*;

import dist.esper.core.comm.Link;
import dist.esper.core.flow.container.DerivedStreamContainer;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.GlobalStat;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

public class CoordinatorStatReportor implements Runnable{
	static Logger2 log=Logger2.getLogger(CoordinatorStatReportor.class);
	long sendIntervalNS;
	long checkStatIntervalMS;
	List<Link> monLinkList=new ArrayList<Link>();
	Map<String, DerivedStreamContainer> containerNameMap=new TreeMap<String, DerivedStreamContainer>();
	Map<Long, DerivedStreamContainer> containerIdMap=new TreeMap<Long, DerivedStreamContainer>();
	GlobalStat gs=new GlobalStat();
	
	Coordinator coordinator;
	public CoordinatorStatReportor(Coordinator coordinator){
		this(coordinator, 4000, 460);
	}

	public CoordinatorStatReportor(Coordinator coordinator,
			long sendIntervalMS, long checkStatIntervalMS) {
		super();
		this.coordinator = coordinator;
		this.sendIntervalNS = sendIntervalMS * 1000000;
		this.checkStatIntervalMS = checkStatIntervalMS;
	}


	@Override
	public void run(){
		long lastSendTimestampNS=System.nanoTime();
		while(true){			
			ThreadUtil.sleep(checkStatIntervalMS);
			long currentTimestampNS=System.nanoTime();
			if( currentTimestampNS - lastSendTimestampNS > sendIntervalNS){
				coordinator.lockContainerMap();
				if(coordinator.costEval.containerStatMap.size() >= coordinator.containerIdMap.size()){
					refreshGlobalStat();
					coordinator.unlockContainerMap();
					sendGlobatStatToMonitors();
					lastSendTimestampNS = currentTimestampNS;
				}
				else{
					coordinator.unlockContainerMap();
				}
			}			
			/*
			ThreadUtil.sleep(sendIntervalNS/1000000);
			coordinator.lockContainerMap();
			refreshGlobalStat();
			coordinator.unlockContainerMap();
			sendGlobatStatToMonitors();
			*/			
		}
//		long timeMS=4000;
//		while(true){
//			ThreadUtil.sleep(timeMS);
//			gs.setEventMap(ServiceManager.getDefaultInstance().getEventRegistry().getEventMap());
//			gs.setWorkerIdMap(ServiceManager.getDefaultInstance().getWorkerIdMap());
//			gs.setRawStreamStatMap(coordinator.costEval.rawStats.getRawStreamStatMap());
//			gs.setContainerNameMap(coordinator.costEval.containerNameMap);
//			gs.setContainerStatMap(coordinator.costEval.containerStatMap);
//			gs.setProcWorkerStatMap(coordinator.costEval.procWorkerStatMap);
//			gs.setGateWorkerStatMap(coordinator.costEval.gateWorkerStatMap);
//			gs.setContainerTreeMap(coordinator.containerTreeMap);
//			gs.setContainerIdMap(coordinator.containerIdMap);
//			monLinkList.clear();
//			monLinkList.addAll(coordinator.monitorLinkMap.values());
//			for(Link link: monLinkList){
//				if(link.isConnected()){
//					int bytes=link.send(gs);
//					//log.debug("*** %s send GlobalStat (%d bytes) to %s\n", coordinator.id, bytes ,link.getTargetId().toString());
//				}
//			}
//		}
	}
	
	public void sendGlobatStatToMonitors(){
		monLinkList.clear();
		monLinkList.addAll(coordinator.monitorLinkMap.values());
		for(Link link: monLinkList){
			try{
				if(!link.isClosed()){
					//coordinator.containersLock.lock();
					int bytes=link.send(gs);
					//coordinator.containersLock.unlock();
				}
			}
			catch(Exception ex){
				log.debug("error ocurr when send GlobatStat", ex);
			}
		}
	}
	
	public void refreshGlobalStat(){
		gs.setEventMap(ServiceManager.getDefaultInstance().getEventRegistry().getEventMap());
		gs.setWorkerIdMap(ServiceManager.getDefaultInstance().getWorkerIdMap());
		gs.setRawStreamStatMap(coordinator.costEval.rawStats.getRawStreamStatMap());		
		gs.setContainerStatMap(coordinator.costEval.containerStatMap);
		gs.setProcWorkerStatMap(coordinator.costEval.procWorkerStatMap);
		gs.setGateWorkerStatMap(coordinator.costEval.gateWorkerStatMap);
		gs.setContainerTreeMap(coordinator.containerTreeMap);
		//gs.setContainerNameMap(coordinator.costEval.containerNameMap);
		//gs.setContainerIdMap(coordinator.containerIdMap);
		
		this.containerNameMap.putAll(coordinator.containerNameMap);
		this.containerIdMap.putAll(coordinator.containerIdMap);
		gs.setContainerNameMap(this.containerNameMap);
		gs.setContainerIdMap(this.containerIdMap);
	}
}
