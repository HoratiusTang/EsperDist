package dist.esper.test;

import java.util.*;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.message.NewStreamInstanceMessage;
import dist.esper.event.Event;
import dist.esper.io.*;

public class TestKryo {
	public static void main(String[] args){
		//test1();
		//test2();
		//test3();
		test4();
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
	
	public static void test4(){
		KryoByteArraySerializer bas=new KryoByteArraySerializer();
		bas.getKryo().register(A.class);
		bas.getKryo().register(B.class);
		
		A a=new A(1);
		B b=new B(2);
		a.setB(b);
		b.setA(a);
		
		byte[] bytes=bas.toBytes(a);
		A a2=(A)bas.fromBytes(bytes);
		System.out.println();
	}
	
	@DefaultSerializer(value = ASerializer.class)
	public static class A{
		String type="A";
		int id=1;
		B b=null;
		public A(){
			
		}
		public A(int id) {
			super();
			this.id = id;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public B getB() {
			return b;
		}
		public void setB(B b) {
			this.b = b;
		}		
	}
	
	public static class ASerializer extends Serializer<A>{

		@Override
		public void write(Kryo kryo, Output output, A a) {
			kryo.writeObject(output, a.type);
			kryo.writeObject(output, a.id);
			kryo.writeObject(output, a.b);
		}

		@Override
		public A read(Kryo kryo, Input input, Class<A> type) {
			A a=new A();
			a.type=kryo.readObject(input, String.class);
			a.id=kryo.readObject(input, Integer.class);
			a.b=kryo.readObject(input, B.class);
			return a;
		}
	}
	
	@DefaultSerializer(value = BSerializer.class)
	public static class B{
		String type="B";
		int id=2;
		A a=null;
		public B(){
			
		}
		public B(int id) {
			super();
			this.id = id;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public A getA() {
			return a;
		}
		public void setA(A a) {
			this.a = a;
		}
		
	}
	public static class BSerializer extends Serializer<B>{
		@Override
		public void write(Kryo kryo, Output output, B b) {
			kryo.writeObject(output, b.type);
			kryo.writeObject(output, b.id);
			kryo.writeObject(output, b.a);
		}

		@Override
		public B read(Kryo kryo, Input input, Class<B> type) {
			B b=new B();
			b.type=kryo.readObject(input, String.class);
			b.id=kryo.readObject(input, Integer.class);
			b.a=kryo.readObject(input, A.class);
			return b;
		}
	}
}
