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
	FlushLinkRunnable flushRun=new FlushLinkRunnable();
	IStatRecorder statRecorder=new DummyStatRecorder();
	public AsyncRawSocketLinkManager(WorkerId myId) {
		super(myId);
		new Thread(flushRun).start();
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
		List<Link> sendLinkList=new ArrayList<Link>();
		List<Link> recvLinkList=new ArrayList<Link>();
		@Override
		public void run() {
			long outputIntervalUS=ServiceManager.getOutputIntervalUS();
			while(true){
				long totalStartTimeNS=System.nanoTime();
				long sendStartTimeNS,sendEndTimeNS;
				int bytes;
				statRecorder.beginRound(sendLinkMap.size()+recvLinkMap.size());
				refreshLinkList(sendLinkMap.values(), sendLinkList);
				for(Link link: sendLinkList){
					if(link instanceof AsyncRawSocketLink){
						sendStartTimeNS=System.nanoTime();
						bytes=((AsyncRawSocketLink)link).flush();
						sendEndTimeNS=System.nanoTime();
						statRecorder.record(link.getLinkId(), bytes, (int)((sendEndTimeNS-sendStartTimeNS)/990.0));
					}
				}
				refreshLinkList(recvLinkMap.values(), recvLinkList);
				for(Link link: recvLinkList){
					if(link instanceof AsyncRawSocketLink){
						sendStartTimeNS=System.nanoTime();
						bytes=((AsyncRawSocketLink)link).flush();
						sendEndTimeNS=System.nanoTime();
						statRecorder.record(link.getLinkId(), bytes, (int)((sendEndTimeNS-sendStartTimeNS)/990.0));
					}
				}
				statRecorder.endRound();
				long totalEndTimeNS=System.nanoTime();
				long sleepTimeMS = (outputIntervalUS>>>10) - ((totalEndTimeNS-totalStartTimeNS)>>>20);
				log.debug("flush all links use %d ms, will sleep %d ms, outputIntervalUS=%d ms", (totalEndTimeNS-totalStartTimeNS)>>>20, sleepTimeMS, outputIntervalUS>>>10);
				if(sleepTimeMS>0){
					ThreadUtil.sleep(sleepTimeMS);
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
		public void endRound();
	}
	
	class DummyStatRecorder implements IStatRecorder{
		@Override public void beginRound(int linkCount) {}
		@Override public void record(long linkId, int sendBytes, long sendTimeUS) {}
		@Override public void endRound() {}
	}
}
