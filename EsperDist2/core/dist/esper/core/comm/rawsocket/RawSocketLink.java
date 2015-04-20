package dist.esper.core.comm.rawsocket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.kryosocket.KryoSocketLink;
import dist.esper.core.id.WorkerId;
import dist.esper.io.KryoByteArraySerializer;
import dist.esper.util.Logger2;

public class RawSocketLink extends Link{
	static Logger2 log=Logger2.getLogger(RawSocketLink.class);
	Socket socket;
	KryoByteArraySerializer bytesSer;
	Sender sender;
	ReceiverRunnable recevierRun;
	ReentrantLock lock=new ReentrantLock();
	byte[] recvBuffer;	
	
	public RawSocketLink(WorkerId myId, WorkerId targetId, Socket socket, int recvBufferSize){
		super(myId, targetId);
		this.socket = socket;
		this.recvBuffer = new byte[recvBufferSize];
		this.bytesSer = new KryoByteArraySerializer(recvBufferSize);
		//init();
	}
	
	public void init(){
		try {
			sender=new Sender();
			recevierRun=new ReceiverRunnable();
			new Thread(recevierRun).start();
			recevierRun.waitStarted();
		}
		catch (Exception e) {			
			log.getLogger().fatal(
					String.format("error occur when init RawSocketLink: targetId=%s", targetId.getId()),e);
		}
	}

	@Override
	public boolean isConnected() {
		return socket.isConnected();
	}

	@Override
	public int send(Object obj) throws RuntimeException {
		try {
			return sender.send(obj);
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	class Sender{
		BufferedOutputStream bos=null;		
		public Sender() throws IOException{
			bos=new BufferedOutputStream(socket.getOutputStream());
		}
		
		public int send(Object obj) throws Exception {
			try{
				lock.lock();
				byte[] bytes=bytesSer.toBytes(obj);
				//OutputStream os=socket.getOutputStream();
				RawSocketLinkUtil.writeLength(bos, bytes.length);
				bos.write(bytes);
				bos.flush();
				return bytes.length;
			}
			catch(Exception ex){				
				throw ex;
			}
			finally{
				lock.unlock();
			}
		}
	}
	
	class ReceiverRunnable implements Runnable{
		BufferedInputStream bis=null;
		Semaphore sem=new Semaphore(0);
		@Override
		public void run(){
			//log.info("SocketThread %d running...\n", id);
			try {
				bis=new BufferedInputStream(socket.getInputStream());
				socket.setKeepAlive(true);
				sem.release();
				while(true){
					boolean isReceived=receive();
					if(!isReceived){
						break;
					}
				}
			}
			catch (Exception e) {
				log.getLogger().error("error occur in ReceiverRunnable", e);
			}
			finally{
				if(bis!=null){
					try{bis.close();} catch(Exception ex){}
				}
			}
		}
		
		public void waitStarted() throws InterruptedException{
			sem.acquire();
		}
		
		public boolean receive() throws Exception{
			int length=RawSocketLinkUtil.readLength(bis);
			//bis.read(recvBuffer, 0, length);
			RawSocketLinkUtil.readBytes(bis, recvBuffer, 0, length);
			Object obj=bytesSer.fromBytes(recvBuffer, 0, length);
			notifyReceived(obj);
			return true;
		}
	}
}
