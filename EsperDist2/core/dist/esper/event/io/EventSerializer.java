package dist.esper.event.io;

import java.util.*;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.event.Event;
import dist.esper.event.EventProperty;

public class EventSerializer extends Serializer<Event> {

	@Override
	public void write(Kryo kryo, Output output, Event event) {
		kryo.writeObject(output, event.getId());
		kryo.writeObject(output, event.getName());
		kryo.writeObject(output, event.getPropList());
	}

	@Override
	public Event read(Kryo kryo, Input input, Class<Event> type) {
		Long id=kryo.readObject(input, Long.class);
		String name=kryo.readObject(input, String.class);
		List<EventProperty> propList=kryo.readObject(input, ArrayList.class);
		Event event=new Event();
		event.setId(id);
		event.setName(name);
		event.setPropList(propList);
		for(EventProperty prop: propList){
			prop.setEvent(event);
			event.put(prop.getName(), prop.getType());
		}
		return event;
	}

}
