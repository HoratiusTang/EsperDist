package dist.esper.core.comm.socket;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage.KeepAlive;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.Link.Listener;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.SubmitEplResponse;
import dist.esper.io.KryoByteArraySerializer;

public class SocketLink extends Link{
	Connection conn=null;
	KryoByteArraySerializer bytesSer=new KryoByteArraySerializer();
	KryonetListener kryonetListener=new KryonetListener();
	
	public SocketLink(WorkerId myId, WorkerId targetId, Connection conn) {
		super(myId, targetId);
		this.conn = conn;
		this.conn.setTimeout(5*60*1000);
		this.conn.addListener(kryonetListener);
	}
	
	@Override
	public int send(Object obj) throws RuntimeException {
		byte[] bytes=bytesSer.toBytes(obj);
		int size=conn.sendTCP(bytes);
		//System.out.format("*** %s sended %s (%d bytes) to %s\n", getMyMeta().getId(), obj.getClass().getSimpleName(), size, getTargetMeta().getId());
		return size;
	}
	
	@Override
	public boolean isConnected() {
		return conn.isConnected();
	}
	
	class KryonetListener extends com.esotericsoftware.kryonet.Listener{
		public void connected(Connection connection) {
			if(connection==SocketLink.this.conn){
				for(Listener ls: overallListenerList){
					ls.connected(SocketLink.this);
				}
			}
		}
		public void disconnected(Connection connection) {
			if(connection==SocketLink.this.conn){
				notifyDisconnnected();
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
}
