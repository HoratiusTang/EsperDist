package dist.esper.core.comm.socket;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.esotericsoftware.kryonet.*;

import dist.esper.core.CoordinatorMain;
import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.comm.Link.Listener;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.KryoClassRegister;
import dist.esper.util.Logger2;

public class SocketLinkManager extends LinkManager {
	static Logger2 log=Logger2.getLogger(SocketLinkManager.class);
	public int writeBufferSize=4096000;
	public int objectBufferSize=2048000;
	protected Server server=null;
	protected Client client=null;
	ServerListener serverListener=new ServerListener();
	
	ConcurrentHashMap<WorkerId, Client2> waitingClientMap=new ConcurrentHashMap<WorkerId, Client2>();
	
	public SocketLinkManager(WorkerId myId) {
		super(myId);
		writeBufferSize=(int)ServiceManager.getConfig().getLong(Options.KRYONET_WRITE_BUFFER_SIZE, writeBufferSize);
		objectBufferSize=(int)ServiceManager.getConfig().getLong(Options.KRYONET_OBJECT_BUFFER_SIZE, objectBufferSize);
	}
	
	@Override
	public void init() {
		server = new Server(writeBufferSize,objectBufferSize);
		KryoClassRegister.registerClasses(server.getKryo());		
		
		try {
			server.addListener(serverListener);
			server.start();
			server.bind(myId.getPort());
		}
		catch (IOException ex) {
			log.getLogger().fatal("fatal error occured, will exit.", ex);
			System.exit(0);
		}
	}
	
	@Override
	public Link connect(WorkerId targetId) {
		Link link=sendLinkMap.get(targetId);
		if(link!=null){
			return link;
		}
		
		if(myId.getId().equals(targetId.getId())){
			link=new LocalLink(myId);
			sendLinkMap.put(myId, link);
			this.notifyNewReceivedLink(link);
			return link;
		}	
		
		Client2 client=new Client2(writeBufferSize, objectBufferSize);
		KryoClassRegister.registerClasses(client.getKryo());
		
		waitingClientMap.putIfAbsent(targetId, client);
		client=waitingClientMap.get(targetId);
		
		try {
			client.lock();
			if(client.isConnected()){
				link=sendLinkMap.get(targetId);
				assert(link!=null);
			}
			else{
				client.start();
				client.connect(100000, targetId.getIp(), targetId.getPort());
				LinkEstablishedMessage lem=new LinkEstablishedMessage(myId);
				client.sendTCP(lem);
				link=new SocketLink(myId, targetId, client, this);
				sendLinkMap.put(targetId, link);
			}
			return link;
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			client.unlock();
		}
		return link;
	}

	@Override
	public Link reconnect(Link oldLink) {
		SocketLink sockLink=(SocketLink)oldLink;
		return sockLink;
	}
	
	class ServerListener extends com.esotericsoftware.kryonet.Listener{
		/**waiting for new connections*/
		public void connected(Connection connection) {
		}
		public void disconnected(Connection connection) {
		}
		public void received(Connection connection, Object obj) {
			if(obj instanceof LinkReconnectMessage){
				LinkReconnectMessage lrm=(LinkReconnectMessage)obj;
				WorkerId targetId=lrm.getWorkerId();
				SocketLink oldRecvLink=(SocketLink) recvLinkMap.get(targetId);
				if(oldRecvLink!=null){
					log.info("%s received LinkReconnectMessage from %s, and old SocketLink is found", myId, targetId);
					oldRecvLink.setNewConnection(connection);
					log.info("%s received LinkReconnectMessage from %s, and old SocketLink is assign new connection", myId, targetId);
				}
				else{
					log.info("%s received LinkReconnectMessage from %s, but old SocketLink is not found", myId, targetId);
				}
			}
			else if(obj instanceof LinkEstablishedMessage){
				LinkEstablishedMessage lem=(LinkEstablishedMessage)obj;
				WorkerId targetId=lem.getWorkerId();
				SocketLink sockLink=new SocketLink(myId, targetId, connection, SocketLinkManager.this);
				notifyNewReceivedLink(sockLink);
			}
		}
	}
	
	/**
	 * internal message after physical link was established, 
	 * before the Link is created,
	 * to inform the remote point its own WorkerId.
	 * @author tjy
	 */
	public static class LinkEstablishedMessage implements Serializable{
		private static final long serialVersionUID = -2924365665356123623L;
		WorkerId workerId;
		public LinkEstablishedMessage(){
			super();
		}
		public LinkEstablishedMessage(WorkerId workerId) {
			super();
			this.workerId = workerId;
		}
		public WorkerId getWorkerId() {
			return workerId;
		}
		public void setWorkerId(WorkerId workerId) {
			this.workerId = workerId;
		}
	}
	
	public static class LinkReconnectMessage extends LinkEstablishedMessage{
		private static final long serialVersionUID = -6617154542485799325L;		
	}
}
