package dist.esper.io;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.cost.WorkerStat;
import dist.esper.core.cost.io.WorkerStatSerializer;
import dist.esper.core.flow.container.DerivedStreamContainer;
import dist.esper.core.flow.container.StreamContainer;
import dist.esper.core.flow.container.StreamContainerFlow;
import dist.esper.core.id.WorkerId;
import dist.esper.event.Event;

@DefaultSerializer(value = GlobalStat.GlobalStatSerializer.class)
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
	
	public static class GlobalStatSerializer extends Serializer<GlobalStat>{
		@Override
		public void write(Kryo kryo, Output output, GlobalStat gs) {
			kryo.writeClassAndObject(output, gs.eventMap);
			kryo.writeClassAndObject(output, gs.workerIdMap);
			kryo.writeClassAndObject(output, gs.procWorkerStatMap);
			kryo.writeClassAndObject(output, gs.gateWorkerStatMap);
			kryo.writeClassAndObject(output, gs.rawStreamStatMap);
			kryo.writeClassAndObject(output, gs.containerStatMap);
			StreamContainer.streamContainersLock.lock();
			kryo.writeClassAndObject(output, gs.containerTreeMap);
			kryo.writeClassAndObject(output, gs.containerNameMap);
			kryo.writeClassAndObject(output, gs.containerIdMap);
			StreamContainer.streamContainersLock.unlock();
		}

		@Override
		public GlobalStat read(Kryo kryo, Input input, Class<GlobalStat> type) {
			GlobalStat gs=new GlobalStat();
			gs.eventMap = (Map<String,Event>) kryo.readClassAndObject(input);
			gs.workerIdMap = (Map<String, WorkerId>) kryo.readClassAndObject(input);
			gs.procWorkerStatMap = (Map<String, WorkerStat>) kryo.readClassAndObject(input);
			gs.gateWorkerStatMap = (Map<String, WorkerStat>) kryo.readClassAndObject(input);
			gs.rawStreamStatMap = (Map<String, RawStreamStat>) kryo.readClassAndObject(input);
			gs.containerStatMap = (Map<String, InstanceStat>) kryo.readClassAndObject(input);
			gs.containerTreeMap = (Map<Long, StreamContainerFlow>) kryo.readClassAndObject(input);
			gs.containerNameMap = (Map<String, DerivedStreamContainer>) kryo.readClassAndObject(input);
			gs.containerIdMap = (Map<Long, DerivedStreamContainer>) kryo.readClassAndObject(input);
			return gs;
		}		
	}
}
