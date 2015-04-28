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
import dist.esper.core.message.ModifyStreamInstanceMessage;
import dist.esper.core.message.NewStreamInstanceMessage;
import dist.esper.io.KryoByteArraySerializer;
import dist.esper.util.BytesFileSerializer;
import dist.esper.util.DateUtil;
import dist.esper.util.Logger2;

public class RawSocketLink extends Link{
	static Logger2 log=Logger2.getLogger(RawSocketLink.class);
	protected Socket socket;
	protected KryoByteArraySerializer bytesSer;
	protected SyncSender sender;
	protected ReceiverRunnable recevierRun;
	protected ReentrantLock lock=new ReentrantLock();
	protected byte[] recvBuffer;	
	
	public RawSocketLink(WorkerId myId, WorkerId targetId, Socket socket, int recvBufferSize, int sendObjectBufferSize){
		super(myId, targetId);
		this.socket = socket;
		this.recvBuffer = new byte[recvBufferSize];
		this.bytesSer = new KryoByteArraySerializer(sendObjectBufferSize);
		//init();
	}
	
	public void init(){
		try {
			sender=new SyncSender();
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
	public boolean isClosed(){
		return socket.isClosed();
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
	
	public class SyncSender{
		BufferedOutputStream bos=null;		
		public SyncSender() throws IOException{
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
				if(obj instanceof NewStreamInstanceMessage || obj instanceof ModifyStreamInstanceMessage){
					BytesFileSerializer.writeBytes(bytes, 0, bytes.length, 
							String.format("message/%s.%s.%s-%s.%s.%d.%s.bin", 
								getMyId().getId(), DateUtil.formatDate(), 
								getMyId().getId(), getTargetId().getId(),
								obj.getClass().getSimpleName(), //class type 
								bytes.length, "sended"));
				}
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
	
	public class ReceiverRunnable implements Runnable{
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
			int length=Integer.MIN_VALUE;
			Object obj=null;
			try{
				length=RawSocketLinkUtil.readLength(bis);
				if(length<0){
					throw new RuntimeException();
				}
				else if(length>0){
					//log.debug("ReceiverRunnable read %d bytes", length);
					RawSocketLinkUtil.readBytes(bis, recvBuffer, 0, length);
					obj=bytesSer.fromBytes(recvBuffer, 0, length);
					notifyReceived(obj);
				}
			}
			catch(Exception ex){
				log.error(String.format("try read length=%d, myId=%s, targetId=%s", length, myId.getId(), targetId.getId()), ex);
				BytesFileSerializer.writeBytes(recvBuffer, 0, length, 
						String.format("message/%s.%s.%s-%s.%s.%d.%s.bin", 
							getMyId().getId(), DateUtil.formatDate(), 
							getTargetId().getId(), getMyId().getId(),
							ex.getMessage(), //class type 
							length, "failed"));
			}
			return true;
		}
	}
}
