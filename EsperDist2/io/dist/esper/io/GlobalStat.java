package dist.esper.io;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.cost.WorkerStat;
import dist.esper.core.flow.container.DerivedStreamContainer;
import dist.esper.core.flow.container.StreamContainerFlow;
import dist.esper.core.id.WorkerId;
import dist.esper.event.Event;

public class GlobalStat implements Serializable{
	private static final long serialVersionUID = 2799939768112516438L;
	
	Map<String,Event> eventMap;
	Map<String, WorkerId> workerIdMap;
	Map<String, WorkerStat> procWorkerStatMap;
	Map<String, WorkerStat> gateWorkerStatMap;
	Map<String, RawStreamStat> rawStreamStatMap;
	Map<String, InstanceStat> containerStatMap;
	Map<Long, StreamContainerFlow> containerTreeMap;
	Map<String, DerivedStreamContainer> containerNameMap;
	Map<Long, DerivedStreamContainer> containerIdMap;
	
	public GlobalStat(){
	}

	public Map<String, Event> getEventMap() {
		return eventMap;
	}

	public void setEventMap(Map<String, Event> eventMap) {
		this.eventMap = eventMap;
	}

	public Map<String, WorkerId> getWorkerIdMap() {
		return workerIdMap;
	}

	public void setWorkerIdMap(Map<String, WorkerId> workerIdMap) {
		this.workerIdMap = workerIdMap;
	}
	
	public Map<String, RawStreamStat> getRawStreamStatMap() {
		return rawStreamStatMap;
	}

	public void setRawStreamStatMap(Map<String, RawStreamStat> rawStreamStatMap) {
		this.rawStreamStatMap = rawStreamStatMap;
	}

	public Map<String, WorkerStat> getProcWorkerStatMap() {
		return procWorkerStatMap;
	}

	public void setProcWorkerStatMap(Map<String, WorkerStat> procWorkerStatMap) {
		this.procWorkerStatMap = procWorkerStatMap;
	}

	public Map<String, WorkerStat> getGateWorkerStatMap() {
		return gateWorkerStatMap;
	}

	public void setGateWorkerStatMap(Map<String, WorkerStat> gateWorkerStatMap) {
		this.gateWorkerStatMap = gateWorkerStatMap;
	}

	public Map<String, InstanceStat> getContainerStatMap() {
		return containerStatMap;
	}

	public void setContainerStatMap(Map<String, InstanceStat> containerStatMap) {
		this.containerStatMap = containerStatMap;
	}

	public Map<String, DerivedStreamContainer> getContainerNameMap() {
		return containerNameMap;
	}

	public void setContainerNameMap(
			Map<String, DerivedStreamContainer> containerMap) {
		this.containerNameMap = containerMap;
	}

	public Map<Long, DerivedStreamContainer> getContainerIdMap() {
		return containerIdMap;
	}

	public void setContainerIdMap(
			Map<Long, DerivedStreamContainer> containerIdMap) {
		this.containerIdMap = containerIdMap;
	}

	public Map<Long, StreamContainerFlow> getContainerTreeMap() {
		return containerTreeMap;
	}

	public void setContainerTreeMap(
			Map<Long, StreamContainerFlow> containerTreeMap) {
		this.containerTreeMap = containerTreeMap;
	}
}
