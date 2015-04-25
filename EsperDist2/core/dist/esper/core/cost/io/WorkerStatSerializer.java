package dist.esper.core.cost.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.cost.WorkerStat;

public class WorkerStatSerializer extends Serializer<WorkerStat> {

	@Override
	public void write(Kryo kryo, Output output, WorkerStat ws) {
		// TODO Auto-generated method stub
		kryo.writeObject(output, ws.id);
		kryo.writeObject(output, ws.isGateway);
		
		kryo.writeObject(output, ws.timestampMS);
		
		kryo.writeObject(output, ws.memUsed);
		kryo.writeObject(output, ws.memFree);
		kryo.writeObject(output, ws.bwUsageUS);
		kryo.writeObject(output, ws.cpuUsage);
		kryo.writeObject(output, ws.cpuCoreCount);
		kryo.writeObject(output, ws.cpuHZ);
		kryo.writeObject(output, ws.procThreadCount);
		kryo.writeObject(output, ws.pubThreadCount);
		
		kryo.writeObject(output, ws.localSubscriberCount);
		kryo.writeObject(output, ws.remoteSubscriberCount);
		kryo.writeObject(output, ws.localPublisherCount);
		kryo.writeObject(output, ws.remotePublisherCount);
		
		kryo.writeObject(output, ws.recvThreadCount);
		
		kryo.writeObject(output, ws.totalProcCount);
		kryo.writeObject(output, ws.recentProcCount);
		kryo.writeObject(output, ws.recentProcIntervalUS);
		kryo.writeObject(output, ws.procTimeUS);
		kryo.writeObject(output, ws.procDelayTimeUS);
		kryo.writeObject(output, ws.currentProcQueueSize);
		
		kryo.writeObject(output, ws.totalPubCount);
		kryo.writeObject(output, ws.recentPubCount);
		kryo.writeObject(output, ws.recentPubIntervalUS);
		kryo.writeObject(output, ws.serialTimeUS);
		kryo.writeObject(output, ws.pubDelayTimeUS);
		kryo.writeObject(output, ws.sendTimeUS);
		kryo.writeObject(output, ws.sendDelayTimeUS);
		kryo.writeObject(output, ws.currentPubQueueSize);
		
		kryo.writeObject(output, ws.sendByteRateUS);
		kryo.writeObject(output, ws.sendBaseTimeUS);
		
		kryo.writeObject(output, ws.filterCondProcTimeUS);
		kryo.writeObject(output, ws.joinCondProcTimeUS);
		kryo.writeObject(output, ws.filterCount);
		kryo.writeObject(output, ws.filterDelayedCount);
		kryo.writeObject(output, ws.patternCount);
		kryo.writeObject(output, ws.patternDelayedCount);
		kryo.writeObject(output, ws.joinCount);
		kryo.writeObject(output, ws.joinDelayedCount);
		kryo.writeObject(output, ws.rootCount);
		
		ws.getInsStatsLock().lock();
		kryo.writeObject(output, ws.insStats);
		ws.getInsStatsLock().unlock();
		
		ws.getRawStatsLock().lock();
		kryo.writeObject(output, ws.rawStats);
		ws.getRawStatsLock().unlock();
	}

	@Override
	public WorkerStat read(Kryo kryo, Input input, Class<WorkerStat> type) {
		WorkerStat ws=new WorkerStat();
		ws.id=kryo.readObject(input, String.class);
		ws.isGateway=kryo.readObject(input, Boolean.class);
		
		ws.timestampMS=kryo.readObject(input, Long.class);
		
		ws.memUsed=kryo.readObject(input, Long.class);
		ws.memFree=kryo.readObject(input, Long.class);
		ws.bwUsageUS=kryo.readObject(input, Double.class);
		ws.cpuUsage=kryo.readObject(input, Double.class);
		ws.cpuCoreCount=kryo.readObject(input, Integer.class);
		ws.cpuHZ=kryo.readObject(input, Long.class);
		ws.procThreadCount=kryo.readObject(input, Integer.class);
		ws.pubThreadCount=kryo.readObject(input, Integer.class);
		
		ws.localSubscriberCount=kryo.readObject(input, Integer.class);
		ws.remoteSubscriberCount=kryo.readObject(input, Integer.class);
		ws.localPublisherCount=kryo.readObject(input, Integer.class);
		ws.remotePublisherCount=kryo.readObject(input, Integer.class);
		
		ws.recvThreadCount=kryo.readObject(input, Integer.class);
		
		ws.totalProcCount=kryo.readObject(input, Long.class);
		ws.recentProcCount=kryo.readObject(input, Long.class);
		ws.recentProcIntervalUS=kryo.readObject(input, Double.class);
		ws.procTimeUS=kryo.readObject(input, Double.class);
		ws.procDelayTimeUS=kryo.readObject(input, Double.class);
		ws.currentProcQueueSize=kryo.readObject(input, Long.class);		
		
		ws.totalPubCount=kryo.readObject(input, Long.class);
		ws.recentPubCount=kryo.readObject(input, Long.class);
		ws.recentPubIntervalUS=kryo.readObject(input, Double.class);
		ws.serialTimeUS=kryo.readObject(input, Double.class);
		ws.pubDelayTimeUS=kryo.readObject(input, Double.class);
		ws.sendTimeUS=kryo.readObject(input, Double.class);
		ws.sendDelayTimeUS=kryo.readObject(input, Double.class);
		ws.currentPubQueueSize=kryo.readObject(input, Long.class);
		
		ws.sendByteRateUS=kryo.readObject(input, Double.class);
		ws.sendBaseTimeUS=kryo.readObject(input, Double.class);
		
		ws.filterCondProcTimeUS=kryo.readObject(input, Double.class);
		ws.joinCondProcTimeUS=kryo.readObject(input, Double.class);		
		ws.filterCount=kryo.readObject(input, Integer.class);
		ws.filterDelayedCount=kryo.readObject(input, Integer.class);
		ws.patternCount=kryo.readObject(input, Integer.class);
		ws.patternDelayedCount=kryo.readObject(input, Integer.class);
		ws.joinCount=kryo.readObject(input, Integer.class);
		ws.joinDelayedCount=kryo.readObject(input, Integer.class);
		ws.rootCount=kryo.readObject(input, Integer.class);
		
		ws.insStats=kryo.readObject(input, InstanceStat[].class);
		ws.rawStats=kryo.readObject(input, RawStreamStat[].class);		
		return ws;
	}

}
