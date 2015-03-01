package dist.esper.core.worker.pubsub;

import java.util.*;
import java.util.concurrent.*;


import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.DataMessage;
import dist.esper.core.message.StartSubscribeMessage;
import dist.esper.core.worker.pubsub.Subscriber.LinkHandler;
import dist.esper.event.Event;

public class RawStreamSampler extends Thread{
	Map<String, RawStreamStat> rawStatMap=new ConcurrentHashMap<String, RawStreamStat>();
	LinkedBlockingQueue<TupleBatch> batchList=new LinkedBlockingQueue<TupleBatch>();
	LinkHandler linkHandler=new LinkHandler();
	LinkManager linkManager=null;
	class LinkHandler implements Link.Listener{
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {}

		@Override
		public void received(Link link, Object obj) {
			if(obj instanceof DataMessage){
				DataMessage dataMsg=(DataMessage)obj;				
				if(rawStatMap.containsKey(dataMsg.getStreamName())){
					submit(dataMsg.getStreamName(), dataMsg.getData());
				}
			}
		}
	}
	
	public RawStreamSampler(LinkManager linkManager) {
		super();
		this.linkManager = linkManager;
	}
	public int size(){
		return rawStatMap.size();
	}
	public Collection<RawStreamStat> getAll(){
		return rawStatMap.values();
	}
	public void sampleNewRawStream(RawStream rsl){
		if(!rawStatMap.containsKey(rsl.getEventName())){
			RawStreamStat rawStat=new RawStreamStat(rsl.getEvent());
			rawStatMap.put(rsl.getEventName(), rawStat);
			Link link=linkManager.connect(rsl.getWorkerId());
			link.addListener(linkHandler, DataMessage.class.getSimpleName());
			StartSubscribeMessage subMsg=new StartSubscribeMessage(
					link.getMyId().getId(),
					rsl.getEventName(),
					rsl.getEventName(),
					rsl.getResultElementUniqueNameList());
			link.send(subMsg);
		}
	}
	public void submit(String name, Object[] data){
		TupleBatch tb=new TupleBatch(name, data);
		try {
			batchList.put(tb);			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		TupleBatch tb=null;
		RawStreamStat rawStat=null;
		while(true){
			try {
				tb=batchList.take();
				rawStat=rawStatMap.get(tb.name);
				rawStat.tryUpdateBatch(tb.data);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class TupleBatch{
		public String name;
		public Object[] data;
		public TupleBatch(String name, Object[] data) {
			super();
			this.name = name;
			this.data = data;
		}
	}
}
