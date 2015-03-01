package dist.esper.util;

import java.lang.reflect.Array;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CyclicQueue<T> {
	public int beginSeq=0;
	public int endSeq=0;//excluded
	public T[] buffer;
	public boolean isSync=true;
	volatile private boolean isLocked=false;
	Class<T> type;
	ReentrantReadWriteLock lock=new ReentrantReadWriteLock();
	
	@SuppressWarnings("unchecked")
	public CyclicQueue(Class<T> type, int capacity){
		this.type=type;
		buffer=(T[]) Array.newInstance(type, capacity);
		for(int i=0;i<buffer.length;i++){
			try {
				buffer[i]=type.newInstance();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public T alloc(){		
		if(buffer.length<=endSeq && beginSeq + buffer.length >= endSeq){
			beginSeq++;
		}
		T t=buffer[endSeq%buffer.length];
		endSeq++;		
		return t;
	}
	
	public T get(int seq){
		return buffer[seq%buffer.length];
	}
	
	public int size(){
		return endSeq-beginSeq;
	}
	
	public T[] getBuffer(){
		return buffer;
	}

	public void setSync(boolean isSync) {
		this.isSync = isSync;
	}
	
	public void lock(){
		lock.writeLock().lock();
		isLocked=true;
	}
	
	public void unlock(){		
		lock.writeLock().unlock();
		isLocked=false;
	}
	
	public boolean isLocked(){
		return isLocked;
	}
}
