package dist.esper.core.comm.rawsocket;

import java.io.*;

public class RawSocketLinkUtil {
	public static byte[] toBytes(int n){
		byte[] b=new byte[4];
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
		assert(b.length==4);
		int n=0;
		n |= ((b[0] & 0x000000FF) << 3*8);
		n |= ((b[1] & 0x000000FF) << 2*8);
		n |= ((b[2] & 0x000000FF) << 1*8);
		n |= (b[3] & 0x000000FF);
		return n;
	}
	public static int readLength(InputStream is) throws IOException{
		byte[] b=new byte[4];
		readBytes(is, b, 0, b.length);
		return toInt(b);
	}
	
	public static int readBytes(InputStream is, byte[] buffer, 
			int offset, int count) throws IOException{
		int curCount=0;
		while(curCount<count){
			curCount += is.read(buffer, offset + curCount, count-curCount);
		}
		return count;
	}
}
