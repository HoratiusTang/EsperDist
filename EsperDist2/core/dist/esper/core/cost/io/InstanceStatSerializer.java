package dist.esper.core.cost.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.cost.InstanceStat.*;

public class InstanceStatSerializer extends Serializer<InstanceStat> {

	@Override
	public void write(Kryo kryo, Output output, InstanceStat insStat) {
		kryo.writeObject(output, insStat.getWorkerId());
		kryo.writeObject(output, insStat.getType());
		kryo.writeObject(output, insStat.getUniqueName());
		
		kryo.writeObject(output, insStat.getInstanceId());
		kryo.writeObject(output, insStat.getProcessorId());
		kryo.writeObject(output, insStat.getStartTimestampUS());
		kryo.writeObject(output, insStat.getLastTimestampUS());
		kryo.writeObject(output, insStat.getBatchCount());
		kryo.writeObject(output, insStat.getEventCount());
		
		kryo.writeObject(output, insStat.getProcTimeUS());
		kryo.writeObject(output, insStat.getOutputIntervalUS());
		
		insStat.getSubLock().readLock().lock();
		kryo.writeObject(output, insStat.getSubStats());
		insStat.getSubLock().readLock().unlock();
		
		insStat.getPubLock().readLock().lock();
		kryo.writeObject(output, insStat.getPubStats());
		insStat.getPubLock().readLock().unlock();
	}

	@Override
	public InstanceStat read(Kryo kryo, Input input, Class<InstanceStat> type) {
		String workerId=kryo.readObject(input, String.class);
		String instanceType=kryo.readObject(input, String.class);
		String uniqueName=kryo.readObject(input, String.class);
		
		Long instanceId=kryo.readObject(input, Long.class);
		Long processorId=kryo.readObject(input, Long.class);
		Long startTimestampUS=kryo.readObject(input, Long.class);
		Long lastTimestampUS=kryo.readObject(input, Long.class);
		Long batchCount=kryo.readObject(input, Long.class);
		Long eventCount=kryo.readObject(input, Long.class);
		
		Double procTimeUS=kryo.readObject(input, Double.class);
		Double outputIntervalUS=kryo.readObject(input, Double.class);
		
		SubscriberStat[] subStats=kryo.readObject(input, SubscriberStat[].class);
		PublisherStat[] pubStats=kryo.readObject(input, PublisherStat[].class);
		
		InstanceStat insStat=new InstanceStat();
		insStat.setWorkerId(workerId);
		insStat.setType(instanceType);
		insStat.setUniqueName(uniqueName);
		
		insStat.setInstanceId(instanceId);
		insStat.setProcessorId(processorId);
		insStat.setStartTimestampUS(startTimestampUS);
		insStat.setLastTimestampUS(lastTimestampUS);
		insStat.setBatchCount(batchCount);
		insStat.setEventCount(eventCount);
		
		insStat.setProcTimeUS(procTimeUS);
		insStat.setOutputIntervalUS(outputIntervalUS);
		
		insStat.setSubStats(subStats);
		insStat.setPubStats(pubStats);
		
		return insStat;
	}

}
