package dist.esper.io;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.coordinator.Coordinator;
import dist.esper.util.Logger2;

public class KryoFileWriter {
	static Logger2 log=Logger2.getLogger(KryoFileWriter.class);
	static Kryo kryo=null;
	public static void initKryo(){
		if(kryo==null){
			kryo=new Kryo();
			KryoClassRegister.registerClasses(kryo);
		}
	}
	public static Object readFromFile(String filePath){
		initKryo();
		try {
			FileInputStream fis = new FileInputStream(filePath);
			Input input=new Input(fis);
			Object obj=kryo.readClassAndObject(input);
			input.close();
			log.debug("read object from %s successfully", filePath);
			return obj;
		}
		catch (Exception e) {
			log.error("read object from %s failed", filePath);
			e.printStackTrace();
		}
		return null;
	}
	
	public static void writeToFile(Object obj, String filePath){
		initKryo();
		try {
			FileOutputStream fos=new FileOutputStream(filePath);
			Output output=new Output(fos);
			kryo.writeClassAndObject(output, obj);
			output.close();
			log.debug("write object to %s successfully", filePath);
		}
		catch (Exception e) {
			log.error("write object to %s failed", filePath);
			e.printStackTrace();
		}		
	}
}
