package dist.esper.test;

import java.util.*;

import dist.esper.event.Event;
import dist.esper.io.*;

public class TestKryo {
	public static void main(String[] args){
		//test1();
		//test2();
		test3();
	}
	
	public static void test1(){
		List<String> list=new ArrayList<String>();
		list.add("a");
		list.add("b");
		
		String filePath="./kryo_list";
		KryoFileWriter.writeToFile(list, filePath);
		
		List<String> list2=(List<String>) KryoFileWriter.readFromFile(filePath);
		System.out.print(list2);
	}
	
	public static void test2(){
		Event a=new Event("A");
		a.addProperty("id", Integer.class);
		a.addProperty("name", String.class);
		a.addProperty("price", Double.class);
		a.addProperty("time", Long.class);
		a.addProperty("clientIds", int[].class);
		
		String filePath="./kryo_event";
		KryoFileWriter.writeToFile(a, filePath);
		
		Event b=(Event)KryoFileWriter.readFromFile(filePath);
		System.out.println(b);
	}
	
	public static void test3(){
		Event a=new Event("A");
		a.addProperty("id", Integer.class);
		a.addProperty("name", String.class);
		a.addProperty("price", Double.class);
		a.addProperty("time", Long.class);
		a.addProperty("clientIds", int[].class);
		
		KryoByteArraySerializer bas=new KryoByteArraySerializer();
		KryoClassRegister.registerClasses(bas.getKryo());
		byte[] bytes=bas.toBytes(a);
		
		Event b=(Event)bas.fromBytes(bytes);
		System.out.println(b);
	}
}
