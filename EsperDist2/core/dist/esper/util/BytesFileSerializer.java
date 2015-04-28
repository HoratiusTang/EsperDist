package dist.esper.util;

import java.io.*;

public class BytesFileSerializer {
	public static void writeBytes(byte[] bytes, int offset, int count, String filePath){
		ObjectOutputStream fout=null;
		try{
			fout=new ObjectOutputStream(new FileOutputStream(filePath));
			fout.writeInt(count);
			fout.write(bytes, offset, count);
			fout.flush();
		}
		catch(Exception ex){			
		}
		finally{
			if(fout!=null){
				try{fout.close();}catch(Exception ex){};
			}
		}
	}
	
	public static int readBytes(String filePath, byte[] buffer) throws Exception{
		File file=new File(filePath);
		return readBytes(file, buffer);
	}
	
	public static int readBytes(File file, byte[] buffer) throws Exception{
		ObjectInputStream fin=null;
		try{
			fin=new ObjectInputStream(new FileInputStream(file));
			int count=fin.readInt();
			int curCount=0;
			while(curCount<count){
				int readCount=fin.read(buffer, curCount, count-curCount);
				curCount += readCount;
			}
			assert(count==curCount);
			return count;
		}
		catch(Exception ex){
			throw ex;
		}
		finally{
			if(fin!=null){
				try{fin.close();}catch(Exception ex){};
			}
		}
	}
}
