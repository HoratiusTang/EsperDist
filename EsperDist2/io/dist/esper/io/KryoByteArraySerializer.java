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
	
	public Kryo getKryo() {
		return kryo;
	}
	
	public byte[] toBytes(Object obj){
		lock.lock();
		while(true){
			try{
				kryo.reset();
				output.setBuffer(buffer);
				kryo.writeClassAndObject(output, obj);
				break;
			}
			catch(IndexOutOfBoundsException ex){
				log.getLogger().error(String.format("kryo bytes buffer size %d is not enough for %s, try to extend length to %d", 
						buffer.length, obj.getClass().getSimpleName(), buffer.length+baseSize), ex);
				buffer=new byte[buffer.length+baseSize];
			}
		}
		lock.unlock();
		byte[] copy=Arrays.copyOf(buffer, output.position());
		return copy;
	}
	
	public int toBytes(Object obj, byte[] bytes){
		return toBytes(obj, bytes, 0);
	}
	
	public int toBytes(Object obj, byte[] bytes, int offset){
		Output out=new Output();
		try{
			kryo.reset();
			out.setBuffer(bytes);
			out.setPosition(offset);
			kryo.writeClassAndObject(out, obj);
			return out.position()-offset;
		}
		catch(Exception ex){
			log.getLogger().error("error occur in toBytes(object, byte[], int)", ex);
			return 0;
		}
	}
	
	public Object fromBytes(byte[] bytes, int offset, int count){
		Input input=new Input();
		input.setBuffer(bytes, offset, count);
		Object obj=kryo.readClassAndObject(input);
		return obj;
	}

	public Object fromBytes(byte[] bytes){
		return this.fromBytes(bytes, 0, bytes.length);
	}
}
