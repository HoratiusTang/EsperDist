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
	Kryo readKryo;
	Kryo writeKryo;
	Output output;
	byte[] buffer;
	ReentrantLock readLock=new ReentrantLock();
	ReentrantLock writeLock=new ReentrantLock();
	
	public KryoByteArraySerializer(){
		this(DEFAULT_BASE_SIZE);
	}
	
	public KryoByteArraySerializer(int baseSize){
		this.baseSize=baseSize;
		this.buffer=new byte[baseSize];
		this.output=new Output();
		this.readKryo=new Kryo();
		this.writeKryo=new Kryo();
		KryoClassRegister.registerClasses(readKryo);
		KryoClassRegister.registerClasses(writeKryo);
	}
	
	public Kryo getReadKryo() {
		return readKryo;
	}
	
	public Kryo getWriteKryo() {
		return writeKryo;
	}
	
	public byte[] getBuffer(){
		return buffer;
	}
	
	public byte[] toBytes(Object obj){
		writeLock.lock();
		while(true){
			try{
				output.setBuffer(buffer);
				writeKryo.reset();
				writeKryo.writeClassAndObject(output, obj);
				try{
					Object obj2=fromBytes(buffer, 0, output.position());
					assert(obj2.getClass().getSimpleName().equals(obj.getClass().getSimpleName()));
				}
				catch(Exception e2){
					log.getLogger().error("********* error occur in toBytes()->fromBytes() *********", e2);
				}
				break;
			}
			catch(IndexOutOfBoundsException ex){
				log.getLogger().error(String.format("kryo bytes buffer size %d is not enough for %s, try to extend length to %d", 
						buffer.length, obj.getClass().getSimpleName(), buffer.length+baseSize), ex);
				buffer=new byte[buffer.length+baseSize];
			}
		}
		writeLock.unlock();
		byte[] copy=Arrays.copyOf(buffer, output.position());
		return copy;
	}
	
	public int toBytes(Object obj, byte[] bytes){
		return toBytes(obj, bytes, 0);
	}
	
	public int toBytes(Object obj, byte[] bytes, int offset){
		try{
			Output out=new Output();
			out.setBuffer(bytes);
			out.setPosition(offset);
			writeLock.lock();
			writeKryo.reset();			
			writeKryo.writeClassAndObject(out, obj);
			int count=out.position()-offset;
			try{
				Object obj2=fromBytes(bytes, offset, count);
				assert(obj2.getClass().getSimpleName().equals(obj.getClass().getSimpleName()));
			}
			catch(Exception e2){
				log.getLogger().error("********* error occur in toBytes()->fromBytes() *********", e2);
			}
			return count;
		}
		catch(Exception ex){
			log.getLogger().error("error occur in toBytes(object, byte[], int)", ex);
			return 0;
		}
		finally{
			writeLock.unlock();
		}
	}
	
	public Object fromBytes(byte[] bytes, int offset, int count){
		Input input=new Input();
		input.setBuffer(bytes, offset, count);
		readLock.lock();
		readKryo.reset();
		Object obj=readKryo.readClassAndObject(input);
		readLock.unlock();
		return obj;
	}

	public Object fromBytes(byte[] bytes){
		return this.fromBytes(bytes, 0, bytes.length);
	}
}
