package dist.esper.core.comm.kryosocket;

import java.util.concurrent.locks.ReentrantLock;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Serialization;

public class Client2 extends Client {
	ReentrantLock lock=new ReentrantLock();
	
	public Client2() {
		super();
	}

	public Client2(int writeBufferSize, int objectBufferSize,
			Serialization serialization) {
		super(writeBufferSize, objectBufferSize, serialization);		
	}

	public Client2(int writeBufferSize, int objectBufferSize) {
		super(writeBufferSize, objectBufferSize);		
	}

	public void lock(){
		lock.lock();
	}
	
	public void unlock(){
		lock.unlock();
	}
}
