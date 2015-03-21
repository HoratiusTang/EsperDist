package dist.esper.core.comm.socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage.KeepAlive;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.Link.Listener;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.SubmitEplResponse;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.KryoByteArraySerializer;
import dist.esper.util.Logger2;

public class SocketLink extends Link{
	static Logger2 log=Logger2.getLogger(SocketLink.class);
	SocketLinkManager linkManager; 
	Connection conn=null;
	KryoByteArraySerializer bytesSer;
	KryonetListener kryonetListener=new KryonetListener();
	ReentrantLock lock=new ReentrantLock();
	Condition reconnectedCond=null;
	
	public SocketLink(WorkerId myId, WorkerId targetId, Connection conn, SocketLinkManager linkManager) {
		super(myId, targetId);
		this.conn = conn;
		this.conn.setTimeout(5*60*1000);
		this.linkManager = linkManager;
		int bufferSize=(int)ServiceManager.getConfig().getLong(Options.KRYONET_WRITE_BUFFER_SIZE, 
				KryoByteArraySerializer.DEFAULT_BASE_SIZE);
		bytesSer=new KryoByteArraySerializer(bufferSize);
		this.conn.addListener(kryonetListener);
	}
	
	@Override
	public int send(Object obj) throws RuntimeException {
		try{
			lock.lock();
			byte[] bytes=bytesSer.toBytes(obj);
			int size=conn.sendTCP(bytes);
			//System.out.format("*** %s sended %s (%d bytes) to %s\n", getMyMeta().getId(), obj.getClass().getSimpleName(), size, getTargetMeta().getId());
			return size;
		}
		catch(RuntimeException ex){
			throw ex;
		}
		finally{
			lock.unlock();
		}
	}
	
	@Override
	public boolean isConnected() {
		return conn.isConnected();
	}
	
	protected void setNewConnection(Connection newConn){
		log.debug("in SocketLink.setNewConnection(), before lock");
		lock.lock();
		log.debug("in SocketLink.setNewConnection(), after lock");
		if(reconnectedCond!=null){
			log.debug("in SocketLink.setNewConnection(), before reconnectedCond.signal()");
			reconnectedCond.signal();
			log.debug("in SocketLink.setNewConnection(), after reconnectedCond.signal()");
		}
		log.debug("in SocketLink.setNewConnection(), before unlock");
		lock.unlock();
		log.debug("in SocketLink.setNewConnection(), after unlock");
	}
	
	public void tryReconnect(Connection breakupConn){
		log.debug("in SocketLink.tryReconnect(), before lock");
		lock.lock();
		log.debug("in SocketLink.tryReconnect(), before unlock");
		if(breakupConn!=this.conn){//check again
			log.debug("in SocketLink.tryReconnect(), Connection is already update, before unlock");
			lock.unlock();
			log.debug("in SocketLink.tryReconnect(), Connection is already update, after unlock");
			return;
		}
		if(conn instanceof Client){
			Client client=(Client)conn;
			try {
				log.debug("in SocketLink.tryReconnect(), before Clinet.reconnect()");
				client.reconnect();
				log.debug("in SocketLink.tryReconnect(), after Clinet.reconnect()");
			}
			catch (IOException e) {
				log.getLogger().error("error occur when reconnecting", e);
			}
		}
		else{
			reconnectedCond=lock.newCondition();
			try {
				log.debug("in SocketLink.tryReconnect(), before reconnectedCond.await()");
				reconnectedCond.await();
				log.debug("in SocketLink.tryReconnect(), after reconnectedCond.await()");
			}
			catch (InterruptedException e) {
				log.getLogger().error("error occur when waiting to be reconnected", e);
			}
		}
		log.debug("in SocketLink.tryReconnect(), before unlock");
		lock.unlock();
		log.debug("in SocketLink.tryReconnect(), after unlock");
	}
		
	
	class KryonetListener extends com.esotericsoftware.kryonet.Listener{
		public void connected(Connection connection) {
			if(connection==SocketLink.this.conn){
				for(Listener ls: overallListenerList){
					ls.connected(SocketLink.this);
				}
				log.info("connnection reconnected: %s", SocketLink.this.toString());
			}
		}
		public void disconnected(Connection connection) {
			if(connection==SocketLink.this.conn){
				notifyDisconnnected();
				log.error("connection disconnected: %s, will try to reconnect", SocketLink.this.toString());
				//tryReconnect(connection);//WRONG, can't be sync
				ReconnectRunnable rr=new ReconnectRunnable(connection);
				new Thread(rr).start();
			}
		}
		public void received(Connection connection, Object obj) {
			if(connection==SocketLink.this.conn && !(obj instanceof KeepAlive)){
				assert(obj instanceof byte[]);
				Object obj2=bytesSer.fromBytes((byte[])obj);
				//System.out.format("*** %s received %s (%d bytes) from %s\n", getMyMeta().getId(), obj2.getClass().getSimpleName(), ((byte[])obj).length, getTargetMeta().getId());
				if(obj2 instanceof SubmitEplResponse){
					System.out.print("");
				}
				notifyReceived(obj2);
			}
		}
	}
	class ReconnectRunnable implements Runnable{
		Connection breakupConn;
		
		public ReconnectRunnable(Connection breakupConn) {
			super();
			this.breakupConn = breakupConn;
		}

		@Override
		public void run() {			
			tryReconnect(breakupConn);
		}		
	}
}
