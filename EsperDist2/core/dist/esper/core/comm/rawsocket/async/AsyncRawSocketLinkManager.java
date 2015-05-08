package dist.esper.core.comm.rawsocket.async;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.rawsocket.RawSocketLink;
import dist.esper.core.comm.rawsocket.RawSocketLinkManager;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.KryoByteArraySerializer;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

public class AsyncRawSocketLinkManager extends RawSocketLinkManager {
	static Logger2 log=Logger2.getLogger(AsyncRawSocketLinkManager.class);
	FlushLinkRunnable flushRun;
	IStatRecorder statRecorder=new DummyStatRecorder();
	public AsyncRawSocketLinkManager(WorkerId myId) {
		super(myId);
		long systemOutputIntervalUS=ServiceManager.getOutputIntervalUS();
		long flushLinkIntervalUS=systemOutputIntervalUS/2;
		flushRun=new FlushLinkRunnable(flushLinkIntervalUS);
		Thread flushThread=new Thread(flushRun);
		flushThread.setPriority(Thread.MAX_PRIORITY);
		flushThread.start();
	}
	
	public IStatRecorder getStatRecorder() {
		return statRecorder;
	}

	public void setStatRecorder(IStatRecorder statRecorder) {
		this.statRecorder = statRecorder;
	}

	@Override
	protected Link newLink(WorkerId myId, WorkerId targetId, Object... args) {
		Socket socket=(Socket)args[0];
		Integer recvBufferSize=(Integer)args[1];
		Integer sendObjectBufferSize=(Integer)args[2];		
		int sendBufferSize=(int)ServiceManager.getConfig().getLong(Options.KRYONET_WRITE_BUFFER_SIZE, 
				KryoByteArraySerializer.DEFAULT_BASE_SIZE*20);
		return new AsyncRawSocketLink(myId, targetId, socket, recvBufferSize,
				sendObjectBufferSize, sendBufferSize);
	}
	
	class FlushLinkRunnable implements Runnable{
		long flushIntervalUS;
		List<Link> sendLinkList=new ArrayList<Link>();
		List<Link> recvLinkList=new ArrayList<Link>();
		
		public FlushLinkRunnable(long flushIntervalUS){
			this.flushIntervalUS = flushIntervalUS;
		}
		
		@Override
		public void run() {			
			long sendStartTimeNS, sendEndTimeNS, totalStartTimeNS, totalEndTimeNS;
			int bytes, roundTotalBytes, asyncLinkCount;
			IStatRecorder curStatRecorder=null;
			long[] t=new long[5];
			StringBuilder sb=new StringBuilder();
			while(true){
				totalStartTimeNS=System.nanoTime();
				curStatRecorder=statRecorder;
				curStatRecorder.beginRound(sendLinkMap.size()+recvLinkMap.size());
				t[0]=totalStartTimeNS;
				refreshLinkList(sendLinkMap.values(), sendLinkList);
				t[1]=System.nanoTime();
				roundTotalBytes=0;
				asyncLinkCount=0;
				for(Link link: sendLinkList){
					if(link instanceof AsyncRawSocketLink){
						asyncLinkCount++;
						try{
							sendStartTimeNS=System.nanoTime();
							bytes=((AsyncRawSocketLink)link).flush();
							roundTotalBytes+=bytes;
							sendEndTimeNS=System.nanoTime();
							statRecorder.record(link.getLinkId(), bytes, (int)((sendEndTimeNS-sendStartTimeNS)/990.0));
						}
						catch(Exception ex){
							log.debug(ex.getMessage(), ex);
						}
					}
				}
				t[2]=System.nanoTime();
				//TEST
				//ThreadUtil.sleep(200);
				refreshLinkList(recvLinkMap.values(), recvLinkList);
				t[3]=System.nanoTime();
				for(Link link: recvLinkList){
					if(link instanceof AsyncRawSocketLink){
						asyncLinkCount++;
						try{
							sendStartTimeNS=System.nanoTime();
							bytes=((AsyncRawSocketLink)link).flush();
							roundTotalBytes+=bytes;
							sendEndTimeNS=System.nanoTime();
							statRecorder.record(link.getLinkId(), bytes, (int)((sendEndTimeNS-sendStartTimeNS)/990.0));
						}
						catch(Exception ex){
							log.debug(ex.getMessage(), ex);
						}
					}
				}
				totalEndTimeNS=System.nanoTime();
				t[4]=totalEndTimeNS;
				long totalSendTimeUS=(totalEndTimeNS-totalStartTimeNS)/1000+1;
				curStatRecorder.endRound(totalSendTimeUS);
				long sleepTimeMS = (flushIntervalUS>>>10) - totalSendTimeUS/1000;
				if(sleepTimeMS>0){
					ThreadUtil.sleep(sleepTimeMS);
				}
				else{
					sb.setLength(0);
					for(int i=0;i<t.length-1;i++){
						sb.append(String.format("t[%d]-t[%d]=%.1f us; ", i+1, i, ((double)(t[i+1]-t[i]))/1000.0));
					}
					log.debug("flush %d links with %d bytes in %d ms, will sleep %d ms, flushIntervalUS=%d ms: %s", 
							asyncLinkCount, roundTotalBytes, totalSendTimeUS/1000, sleepTimeMS, flushIntervalUS>>>10,
							sb.toString());
				}
			}
		}
		
		public void refreshLinkList(Collection<Link> links, List<Link> linkList){
			if(links.size() != linkList.size()){
				linkList.clear();
				linkList.addAll(links);
			}
		}
	}
	
	public interface IStatRecorder{
		public void beginRound(int linkCount);
		public void record(long linkId, int sendBytes, long sendTimeUS);
		public void endRound(long totalSendTimeUS);
	}
	
	class DummyStatRecorder implements IStatRecorder{
		@Override public void beginRound(int linkCount) {}
		@Override public void record(long linkId, int sendBytes, long sendTimeUS) {}
		@Override public void endRound(long totalSendTimeUS) {}
	}
}
