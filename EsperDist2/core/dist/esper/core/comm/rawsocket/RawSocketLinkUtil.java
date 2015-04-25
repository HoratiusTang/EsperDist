package dist.esper.core.comm.rawsocket;

import java.io.*;

public class RawSocketLinkUtil {
	public static int LENGTH_SIZE=4;
	public static byte[] toBytes(int n){
		byte[] b=new byte[LENGTH_SIZE];
		b[0] = (byte)((n & 0xFF000000) >>> 3*8);
		b[1] = (byte)((n & 0x00FF0000) >>> 2*8);
		b[2] = (byte)((n & 0x0000FF00) >>> 1*8);
		b[3] = (byte)((n & 0x000000FF));
		return b;
	}
	public static void writeLength(OutputStream os, int length) throws IOException{
		byte[] b=toBytes(length);
		os.write(b);
	}
	
	public static int toInt(byte[] b){
		assert(b.length==LENGTH_SIZE);
		int n=0;
		n |= ((b[0] & 0x000000FF) << 3*8);
		n |= ((b[1] & 0x000000FF) << 2*8);
		n |= ((b[2] & 0x000000FF) << 1*8);
		n |= (b[3] & 0x000000FF);
		return n;
	}
	public static int readLength(InputStream is) throws Exception{
		byte[] b=new byte[LENGTH_SIZE];
		readBytes(is, b, 0, b.length);
		return toInt(b);
	}
	
	public static int readBytes(InputStream is, byte[] buffer, 
			int offset, int count) throws Exception{
		int curCount=0;
		try{
			while(curCount<count){
				int readCount=is.read(buffer, offset + curCount, count-curCount);
				if(readCount>=0){
					curCount += readCount;
				}
				else{
					throw new RuntimeException(String.format("read() return %d, socket might be closed", readCount));
				}
			}
		}
		catch(Exception ex){
			if(ex instanceof IndexOutOfBoundsException){
				throw new RuntimeException(
						String.format("offset=%d, count=%d, buffer.length=%d", offset, count, buffer.length), ex);
			}
		}
		return count;
	}
}
