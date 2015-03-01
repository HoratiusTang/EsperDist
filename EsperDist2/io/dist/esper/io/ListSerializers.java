package dist.esper.io;

import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.event.Event;

public class ListSerializers{
	public static class ArrayListSerializer extends Serializer<java.util.ArrayList>{

		@Override
		public void write(Kryo kryo, Output output, ArrayList list) {
			kryo.writeObject(output, Integer.valueOf(list.size()));
			for(Object obj: list){
				//kryo.writeObject(output, obj);
				kryo.writeClassAndObject(output, obj);
			}
		}

		@Override
		public ArrayList read(Kryo kryo, Input input, Class<ArrayList> type) {
			Integer size=kryo.readObject(input, Integer.class);
			ArrayList list=new ArrayList(size);
			for(int i=0;i<size;i++){
				Registration reg=kryo.readClass(input);
				Object obj=kryo.readObject(input, reg.getType());
				list.add(obj);
			}
			return list;
		}		
	}
}
