package dist.esper.core.coordinator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.flow.container.DerivedStreamContainer;
import dist.esper.core.flow.container.StreamContainer;
import dist.esper.core.flow.container.StreamContainerFlow;
import dist.esper.util.Logger2;

public class DelayedStreamContainerFlowRegistry {
	static Logger2 log=Logger2.getLogger(Coordinator.class);
	Coordinator coordinator;

	public DelayedStreamContainerFlowRegistry(Coordinator coordinator) {
		super();
		this.coordinator = coordinator;
	}
	
	Map<String, ContainerFlag> containerFlagMap=new ConcurrentHashMap<String, ContainerFlag>();
	Map<Long, ContainerFlowFlag> containerFlowFlagSet=new ConcurrentHashMap<Long, ContainerFlowFlag>();
	public void delayNewStreamContainerFlow(StreamContainerFlow scf){
		List<StreamContainer> containerList=scf.dumpAllUpStreamContainers();
		coordinator.lockContainerMap();
		for(StreamContainer container: containerList){
			coordinator.containerNameMap.put(container.getUniqueName(), (DerivedStreamContainer)container);
			coordinator.containerIdMap.put(container.getId(), (DerivedStreamContainer)container);				
		}
		coordinator.unlockContainerMap();
		
		ContainerFlowFlag cff=new ContainerFlowFlag(scf.getEplId(), containerList);
		containerFlowFlagSet.put(cff.eqlId, cff);
		for(ContainerFlag cf: cff.containerFlags){
			containerFlagMap.put(cf.getContainerName(), cf);
		}
	}
	
	public void checkContainerAppeared(InstanceStat[] insStats){
		if(containerFlagMap.size()<=0){
			return;
		}
		for(InstanceStat insStat: insStats){
			ContainerFlag cf=containerFlagMap.get(insStat.getUniqueName());
			if(cf!=null && cf.markAppeared()){
				ContainerFlowFlag cff=cf.containerFlowFlag;
				cff.incAppearedCount();
				if(cff.allAppeared()){
					registStreamContainerFlow(cff);
				}
			}
		}
	}
	
	public void registStreamContainerFlow(ContainerFlowFlag cff){
		log.debug("regist StreamContainerFlow with eqlId="+cff.eqlId);
		for(ContainerFlag cf: cff.containerFlags){
			containerFlagMap.remove(cf.getContainerName());
			coordinator.addToExistedStreamContainer(cf.container);
			//gc
			cf.containerFlowFlag=null;
		}
		containerFlowFlagSet.remove(cff.eqlId);
		//gc
		cff.containerFlags=null;
		
	}
	class ContainerFlowFlag{
		long eqlId;
		ContainerFlag[] containerFlags;
		AtomicInteger appearedCount=new AtomicInteger(0);
		public ContainerFlowFlag(long eqlId, List<StreamContainer> containerList){
			this.eqlId=eqlId;
			containerFlags=new ContainerFlag[containerList.size()];
			for(int i=0;i<containerFlags.length;i++){
				containerFlags[i]=new ContainerFlag(containerList.get(i), this);
			}
		}
		
		public boolean allAppeared(){
			return appearedCount.intValue()==containerFlags.length;
		}
		public void incAppearedCount(){
			appearedCount.incrementAndGet();
		}
	}
	
	class ContainerFlag{
		ContainerFlowFlag containerFlowFlag;
		StreamContainer container;
		AtomicBoolean appeared=new AtomicBoolean(false);
		public ContainerFlag(StreamContainer container, ContainerFlowFlag cntFlowFlag){
			this.container=container;
			this.containerFlowFlag=cntFlowFlag;				
		}
		public boolean markAppeared(){
			return appeared.compareAndSet(false, true);
		}
		public String getContainerName(){
			return container.getUniqueName();
		}
	}
}
