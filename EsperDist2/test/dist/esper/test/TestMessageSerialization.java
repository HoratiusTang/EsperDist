package dist.esper.test;

import java.io.File;
import java.util.List;

import dist.esper.io.KryoByteArraySerializer;
import dist.esper.io.KryoClassRegister;
import dist.esper.util.BytesFileSerializer;

public class TestMessageSerialization {
	static KryoByteArraySerializer bas;
	public static void main(String[] args){
		String filePath="message/coordinator.0428134916.coordinator-Worker3.NewStreamInstanceMessage.1537.sended.bin";
		//run(new String[]{filePath});
		run(args);
	}
	public static void run(String[] args){
		if(args.length<1){
			System.out.println("error: please input folder name or file name.");
			return;
		}
		File file=new File(args[0]);
		if(!file.exists()){
			System.out.format("error: file '%s' does not exist\n", args[0]);
			return;
		}
		bas=new KryoByteArraySerializer(20480000);
		KryoClassRegister.registerClasses(bas.getReadKryo());
		KryoClassRegister.registerClasses(bas.getWriteKryo());
		if(file.isDirectory()){
			File[] files=file.listFiles();
			for(File f: files){
				deserialize(f);
			}
		}
		else{
			deserialize(file);
		}
	}
	public static Object deserialize(File file){
		System.out.format("------------- %s -------------\n", file.getName());
		try{
			int count=BytesFileSerializer.readBytes(file, bas.getBuffer());
			Object obj=bas.fromBytes(bas.getBuffer(), 0, count);
			if(obj!=null){
				System.out.format("success deserialize %s\n", obj.getClass().getSimpleName());
			}
			return obj;
		}
		catch(Exception ex){
			ex.printStackTrace(System.out);
			return null;
		}
	}
	
}
