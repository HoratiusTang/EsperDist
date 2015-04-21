package dist.esper.core.comm.rawsocket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.locks.ReentrantLock;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.comm.local.LocalLink;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.KryoByteArraySerializer;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

public class RawSocketLinkManager extends LinkManager {
	static int MAX_TRY_SEND_TIMES=10;
	static Logger2 log=Logger2.getLogger(RawSocketLinkManager.class);
	KryoByteArraySerializer bytesSer;
	Server server;
	ReentrantLock lock=new ReentrantLock();
	
	public RawSocketLinkManager(WorkerId myId) {
		super(myId);
		bytesSer=new KryoByteArraySerializer(getBufferSize());
	}
	
	public int getBufferSize(){
		return (int)ServiceManager.getConfig().getLong(Options.KRYONET_OBJECT_BUFFER_SIZE, 
				KryoByteArraySerializer.DEFAULT_BASE_SIZE);
	}

	@Override
	public void init() {
		try {
			server=new Server();
			server.init();
			new Thread(server).start();
		}
		catch (IOException e) {			
			log.getLogger().error("error ocurr when init server", e);
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
		try{
			lock.lock();
			Socket socket=new Socket();
			InetSocketAddress targetAddr=new InetSocketAddress(targetId.getIp(), targetId.getPort());		
			int tryCount=0;
			while(tryCount<MAX_TRY_SEND_TIMES){
				try {
					socket.connect(targetAddr, 5000);
					if(socket.isConnected()){
						byte[] sendBuffer=new byte[5120];
						OutputStream os=socket.getOutputStream();
						LinkEstablishedMessage lem=new LinkEstablishedMessage(myId);
						int length=bytesSer.toBytes(lem, sendBuffer);
						RawSocketLinkUtil.writeLength(os, length);
						os.write(sendBuffer, 0, length);
						os.flush();
						log.info("%s connected to %s", myId.getId(), targetId.getId());
						//RawSocketLink sockLink=new RawSocketLink(myId, targetId, socket, getBufferSize());
						RawSocketLink sockLink=(RawSocketLink)newLink(myId, targetId, socket, getBufferSize(), getBufferSize());
						sockLink.init();
						sendLinkMap.put(targetId, sockLink);
						return sockLink;
					}
				}
				catch (IOException e) {
					ThreadUtil.sleep(5000);
				}			
				tryCount++;
			}
		}
		catch(Exception ex){
			log.getLogger().error("error ocurr when connect to server", ex);
		}
		finally{
			lock.unlock();
		}
		return null;
	}

	@Override
	public Link reconnect(Link oldLink) {		
		return oldLink;
	}
	
	public class Server implements Runnable{		
		public ServerSocket serverSocket=null;	
		public boolean bStopFlag=false;
		public byte[] recvBuffer;

		public Server() {
			super();			
			this.recvBuffer = new byte[5120];
		}
		
		public void init() throws IOException{
			serverSocket=new ServerSocket(myId.getPort());
			log.info("server for %s listening on port %d...\n", myId.getId(), myId.getPort());
		}

		@Override
		public void run(){
			while(!bStopFlag){				
				try {
					boolean isAccepted=acceptConnection();
					if(!isAccepted){
						break;
					}
				}
				catch (IOException e) {
					log.getLogger().error("error ocurr when ServerSocket.accept()", e);
				}
			}
			try {
				serverSocket.close();
			} catch (IOException e) {
				log.getLogger().error("error ocurr when ServerSocket.close()", e);
			}
			log.info("server stoped");
		}
		
		public boolean acceptConnection() throws IOException{
			Socket socket = serverSocket.accept();
			if(socket==null || !socket.isConnected()){
				return false;
			}
			//log.info("server accept connection from ip %s\n", socket.getInetAddress());
			InputStream is=socket.getInputStream();
			int length=RawSocketLinkUtil.readLength(is);
			//is.read(recvBuffer, 0, length);
			RawSocketLinkUtil.readBytes(is, recvBuffer, 0, length);
			Object obj=bytesSer.fromBytes(recvBuffer, 0, length);
			if(obj instanceof LinkEstablishedMessage){
				LinkEstablishedMessage lem=(LinkEstablishedMessage)obj;
				WorkerId targetId=lem.getWorkerId();				
				//RawSocketLink sockLink=new RawSocketLink(myId, targetId, socket, getBufferSize());
				RawSocketLink sockLink=(RawSocketLink)newLink(myId, targetId, socket, getBufferSize(), getBufferSize());
				log.info("server accept connection from %s", targetId.getId());
				notifyNewReceivedLink(sockLink);
				sockLink.init();
				return true;
			}
			else{
				return false;
			}
		}
	}

	@Override
	protected Link newLink(WorkerId myId, WorkerId targetId, Object... args) {
		Socket socket=(Socket)args[0];
		Integer recvBufferSize=(Integer)args[1];
		Integer sendObjectBufferSize=(Integer)args[2];
		return new RawSocketLink(myId, targetId, socket, recvBufferSize, sendObjectBufferSize);
	}
}
/**
 * internal message after physical link was established, 
 * before the Link is created,
 * to inform the remote point its own WorkerId.
 * @author tjy
 */
class LinkEstablishedMessage implements Serializable{
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

class LinkReconnectMessage extends LinkEstablishedMessage{
	private static final long serialVersionUID = -6617154542485799325L;		
}
