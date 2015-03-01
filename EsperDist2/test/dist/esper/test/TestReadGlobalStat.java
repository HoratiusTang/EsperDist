package dist.esper.test;

import dist.esper.io.GlobalStat;
import dist.esper.io.KryoFileWriter;

public class TestReadGlobalStat {
	public static void main(String[] args){
		String filePath="globalstat.bin";
		//GlobalStat gs=GlobalStat.readFromFile(filePath);
		//String filePath="eventmap.bin";
		//String filePath="eventprop.bin";
		Object obj=KryoFileWriter.readFromFile(filePath);
		System.out.println("read finish");
	}
}

