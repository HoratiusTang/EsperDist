package dist.esper.core.comm.rawsocket.async;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import dist.esper.core.comm.rawsocket.RawSocketLink;
import dist.esper.core.comm.rawsocket.RawSocketLinkUtil;
import dist.esper.core.id.WorkerId;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

public class AsyncRawSocketLink extends RawSocketLink {
	static Logger2 log=Logger2.getLogger(RawSocketLink.class);
	AsyncSender sendBuffer;
	public AsyncRawSocketLink(WorkerId myId, WorkerId targetId, Socket socket,
			int recvBufferSize, int sendObjectBufferSize, int sendBufferSize) {
		super(myId, targetId, socket, recvBufferSize, 8);
		sendBuffer=new AsyncSender(sendObjectBufferSize, sendBufferSize);
	}
	
	@Override	
	public void init(){
		try {
			sendBuffer.setOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			recevierRun=new ReceiverRunnable();
			new Thread(recevierRun).start();
			recevierRun.waitStarted();
		}
		catch (Exception e) {
			log.getLogger().fatal(
					String.format("error occur when init AsyncRawSocketLink: targetId=%s", targetId.getId()),e);
		}
	}
	
	@Override
	public int send(Object obj){
		if(socket.isClosed()){
			log.error("send() failed, socket is closed between %s with %s", 
					myId.getId(), targetId.getId());
			return 0;
		}
		lock.lock();
		int size=sendBuffer.append(obj);
		lock.unlock();
		return size;
	}
	
	public int flush() throws Exception{
		if(socket.isClosed()){
			log.error("flush() failed, socket is closed between %s with %s", 
					myId.getId(), targetId.getId());
			return 0;
		}
		int size=sendBuffer.flush();
		return size;
	}
	
	class AsyncSender{
		private byte[] objectBuf;
		byte[] array;
		volatile long begin;	//begin of current buffer (included)
		volatile long end;	//end of current buffer (excluded), begin of coming buffer (included)
		volatile long newEnd;	//excluded
		OutputStream out;
		
		public AsyncSender(int sendObjectBufferSize, int sendBufferSize){
			this(sendObjectBufferSize, sendBufferSize, null);
		}
		
		public AsyncSender(int sendObjectBufferSize, int sendBufferSize, OutputStream out){
			this.out=out;
			this.objectBuf=new byte[sendObjectBufferSize];
			this.array=new byte[sendBufferSize];
			this.begin=0;
			this.end=0;
			this.newEnd=0;
		}
		
		public int append(Object obj){
			int objectBufLength=bytesSer.toBytes(obj, objectBuf, RawSocketLinkUtil.LENGTH_SIZE);
			int totalBufLength=objectBufLength+RawSocketLinkUtil.LENGTH_SIZE;
			//length += RawSocketLinkUtil.LENGTH_SIZE;
			byte[] lengthBytes=RawSocketLinkUtil.toBytes(objectBufLength);
			System.arraycopy(lengthBytes, 0, objectBuf, 0, lengthBytes.length);
			long oldEnd=end;
//			log.debug("append %s, before acquire: begin=%d, end=%d, oldEnd=%d, newEnd=%d, length=%d",
//					obj.getClass().getSimpleName(), begin, end, oldEnd, newEnd, objectBufLength);
			if(require(totalBufLength)){
//				log.debug("append %s, after acquire: begin=%d, end=%d, oldEnd=%d, newEnd=%d",
//						obj.getClass().getSimpleName(), begin, end, oldEnd, newEnd);
				newEnd=oldEnd+totalBufLength;
				if(newEnd / array.length == oldEnd / array.length){
					System.arraycopy(objectBuf, 0, array, (int)(oldEnd % array.length), totalBufLength);
				}
				else{
					int firstSegLength =  array.length - (int)(oldEnd % array.length);
					if(firstSegLength>0){
						System.arraycopy(objectBuf, 0, array, (int)(oldEnd % array.length), firstSegLength);
					}
					System.arraycopy(objectBuf, firstSegLength, array, 0, totalBufLength-firstSegLength);
					log.debug("append in two segments: %d=%d+%d", totalBufLength, 
							firstSegLength, totalBufLength-firstSegLength);
				}
				end=newEnd;
//				log.debug("append %s, before return: begin=%d, end=%d, oldEnd=%d, newEnd=%d",
//						obj.getClass().getSimpleName(), begin, end, oldEnd, newEnd);
				return totalBufLength;
			}
			return 0;
		}
		
		public int flush() throws Exception{
			if(out==null || end==begin){
				return 0;
			}
			//log.debug("before flush");
			long oldEnd=end;
			long oldBegin=begin;
			try {				
				if(oldEnd / array.length == oldBegin / array.length){
					out.write(array, (int)(oldBegin % array.length), (int)(oldEnd-oldBegin));
				}
				else{
					int firstSegLength =  array.length - (int)(oldBegin % array.length);
					if(firstSegLength>0){
						out.write(array, (int)(oldBegin % array.length), firstSegLength);
					}
					out.write(array, 0, (int)(oldEnd % array.length));
					log.debug("flush in two segments: %d=%d+%d", (oldEnd-oldBegin), 
							firstSegLength, oldEnd-oldBegin-firstSegLength);
				}
				out.flush();//MUST!
				begin=oldEnd;
				//log.debug("flushed %d bytes", (int)(oldEnd-oldBegin));
				return (int)(oldEnd-oldBegin);
			}
			catch (IOException e) {
				Exception ex=new Exception(String.format("error occur when write to OutputStream: begin=%d, end=%d, oldEnd=%d, newEnd=%d",
								begin, end, oldEnd, newEnd), e);
				throw ex;
			}
		}
		
		public void setOutputStream(OutputStream out){
			this.out=out;
		}
		
		public boolean require(int size){
			if(size>array.length){
				return false;
			}
			while( begin+array.length < end+size){
				ThreadUtil.sleep(300);
			}
			return true;
		}
	}
}
