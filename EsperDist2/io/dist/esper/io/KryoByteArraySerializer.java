package dist.esper.io;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.comm.kryosocket.KryoSocketLink;
import dist.esper.util.Logger2;

public class KryoByteArraySerializer{
	static Logger2 log=Logger2.getLogger(KryoByteArraySerializer.class);
	public static final int DEFAULT_BASE_SIZE=4096000;
	int baseSize;
	Kryo kryo=new Kryo();;
	ReentrantLock lock=new ReentrantLock();
	Output output;
	byte[] buffer;
	
	public KryoByteArraySerializer(){
		this(DEFAULT_BASE_SIZE);
	}
	
	public KryoByteArraySerializer(int baseSize){
		this.baseSize=baseSize;
		this.buffer=new byte[baseSize];
		this.output=new Output();
	}
	
	public byte[] toBytes(Object obj){
		lock.lock();
		while(true){
			try{
				output.setBuffer(buffer);
				kryo.writeClassAndObject(output, obj);
				break;
			}
			catch(IndexOutOfBoundsException ex){
				log.getLogger().error(String.format("kryo bytes buffer size %d is not enough, try to extend length to %d", 
						buffer.length, buffer.length+baseSize), ex);
				buffer=new byte[buffer.length+baseSize];
			}
		}
		lock.unlock();
		byte[] copy=Arrays.copyOf(buffer, output.position());
		return copy;
	}
	
	public Kryo getKryo() {
		return kryo;
	}

	public Object fromBytes(byte[] bytes){
		Input input=new Input();
		input.setBuffer(bytes);
		Object obj=kryo.readClassAndObject(input);
		return obj;
	}
}
