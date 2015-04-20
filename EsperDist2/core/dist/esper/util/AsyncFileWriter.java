package dist.esper.util;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncFileWriter {
	static SimpleDateFormat sdf=new SimpleDateFormat("MMddHHmmss");
	int capacity=1024*100;
	String filePath;
	long intervalMS;
	ReentrantLock lock=new ReentrantLock();
	FlushRunnable flushRun=new FlushRunnable();
	StringBuilder sb;
	FileWriter fileWriter;
	
	public AsyncFileWriter(String filePathBase, long intervalMS) {
		super();
		//this.filePathBase = filePathBase;
		this.intervalMS = intervalMS;
		this.sb=new StringBuilder(capacity);
		init(filePathBase);
	}
	
	public void init(String filePathBase){
		filePath=filePathBase+"."+sdf.format(new Date())+".txt";
		try {
			fileWriter=new FileWriter(filePath);
			new Thread(flushRun).start();
		}
		catch (Exception e) {			
			e.printStackTrace();
		}
	}
	
	public void append(String str){
		lock.lock();
		if(sb.capacity()<str.length()){
			flush();
		}
		sb.append(str);
		lock.unlock();
	}

	public void flush(){		
		if(sb.length()>0){
			try {
				fileWriter.write(sb.toString());
				fileWriter.flush();
				sb.setLength(0);
			}
			catch (IOException e) {				
				e.printStackTrace();
			}
		}
	}
	
	class FlushRunnable implements Runnable{
		@Override
		public void run() {
			while(true){
				ThreadUtil.sleep(intervalMS);
				if(sb.length()>0){
					lock.lock();
					flush();
					lock.unlock();
				}
			}
		}
	}
}
