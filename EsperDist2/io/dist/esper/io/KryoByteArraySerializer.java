package dist.esper.io;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoByteArraySerializer{
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
