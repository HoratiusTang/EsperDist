package dist.esper.monitor.ui.data;

import java.util.*;

import dist.esper.core.cost.CostEvaluator;
import dist.esper.core.cost.InstanceStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.*;
import dist.esper.core.id.WorkerId;
import dist.esper.util.Logger2;

public class ContainerListWrapper {
	static Logger2 log=Logger2.getLogger(CostEvaluator.class);
	WorkerId workerId;	
	List<FilterStreamContainer> fscList;
	List<JoinStreamContainer> jscList;
	List<FilterDelayedStreamContainer> fdscList;
	List<JoinDelayedStreamContainer> jdscList;
	List<RootStreamContainer> rscList;
	Map<String, InstanceStat> containerStatMap;
	StreamContainerComparator scComp=new StreamContainerComparator(); 
	
	public ContainerListWrapper(WorkerId workerId, Map<String, InstanceStat> containerStatMap) {
		super();
		this.workerId = workerId;
		this.containerStatMap = containerStatMap;
		fscList=new ArrayList<FilterStreamContainer>();
		fdscList=new ArrayList<FilterDelayedStreamContainer>();
		jscList=new ArrayList<JoinStreamContainer>();
		jdscList=new ArrayList<JoinDelayedStreamContainer>();
		rscList=new ArrayList<RootStreamContainer>();
	}
	
	public ContainerListWrapper(WorkerId workerId,
			List<FilterStreamContainer> fscList,
			List<JoinStreamContainer> jscList,
			List<FilterDelayedStreamContainer> fdscList,
			List<JoinDelayedStreamContainer> jdscList,
			List<RootStreamContainer> rscList,
			Map<String, InstanceStat> containerStatMap) {
		super();
		this.workerId = workerId;
		this.fscList = fscList;
		this.jscList = jscList;
		this.fdscList = fdscList;
		this.jdscList = jdscList;
		this.rscList = rscList;
		this.containerStatMap = containerStatMap;
	}
	
	public <T extends StreamContainer> List<T>[] getSortedLists(){
		Collections.sort(fscList, scComp);
		Collections.sort(fdscList, scComp);
		Collections.sort(jscList, scComp);
		Collections.sort(jdscList, scComp);
		Collections.sort(rscList, scComp);
		return (List<T>[]) new List<?>[]{fscList, fdscList, jscList, jdscList, rscList};
	}
	
	public void addStreamContainer(StreamContainer sc){
		if(sc instanceof FilterDelayedStreamContainer){
			fdscList.add((FilterDelayedStreamContainer)sc);
		}
		else if(sc instanceof FilterStreamContainer){
			fscList.add((FilterStreamContainer)sc);
		}
		else if(sc instanceof JoinDelayedStreamContainer){
			jdscList.add((JoinDelayedStreamContainer)sc);
		}
		else if(sc instanceof JoinStreamContainer){
			jscList.add((JoinStreamContainer)sc);
		}
		else if(sc instanceof RootStreamContainer){
			rscList.add((RootStreamContainer)sc);
		}
	}
	public WorkerId getWorkerId() {
		return workerId;
	}
	public void setWorkerId(WorkerId workerId) {
		this.workerId = workerId;
	}
	public List<FilterStreamContainer> getFilterContainerList() {
		return fscList;
	}
	public void setFilterContainerList(List<FilterStreamContainer> fscList) {
		this.fscList = fscList;
	}
	public List<JoinStreamContainer> getJoinContainerList() {
		return jscList;
	}
	public void setJoinContainerList(List<JoinStreamContainer> jscList) {
		this.jscList = jscList;
	}
	public List<FilterDelayedStreamContainer> getFilterDelayedContainerList() {
		return fdscList;
	}
	public void setFilterDelayedContainerList(List<FilterDelayedStreamContainer> fdscList) {
		this.fdscList = fdscList;
	}
	public List<JoinDelayedStreamContainer> getJoinDelayedContainerList() {
		return jdscList;
	}
	public void setJoinDelayedContainerList(List<JoinDelayedStreamContainer> jdscList) {
		this.jdscList = jdscList;
	}
	public List<RootStreamContainer> getRootContainerList() {
		return rscList;
	}
	public void setRootContainerList(List<RootStreamContainer> rscList) {
		this.rscList = rscList;
	}

	public Map<String, InstanceStat> getContainerStatMap() {
		return containerStatMap;
	}

	public void setContainerStatMap(Map<String, InstanceStat> containerStatMap) {
		this.containerStatMap = containerStatMap;
	}
	
	public InstanceStat getContainerStat(String streamName){
		InstanceStat insStat=containerStatMap.get(streamName);
		try{
			if(insStat==null)
				throw new NullPointerException();
		}
		catch(Exception ex){
			log.error("InstanceStat is null with stream name "+streamName, ex);
		}
		return insStat;
	}
	
	class StreamContainerComparator implements Comparator<DerivedStreamContainer>{
		@Override
		public int compare(DerivedStreamContainer sc1, DerivedStreamContainer sc2) {
			InstanceStat insStat1=getContainerStat(sc1.getUniqueName());
			InstanceStat insStat2=getContainerStat(sc2.getUniqueName());
			double d=insStat1.getOutputRateSec()-insStat2.getOutputRateSec();
			if(d<0.0)
				return 1;//reversed
			else if(d>0.0){
				return -1;
			}
			return 0;
		}
	}
}
