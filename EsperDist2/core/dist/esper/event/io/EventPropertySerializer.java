package dist.esper.event.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.event.Event;
import dist.esper.event.EventProperty;

public class EventPropertySerializer extends Serializer<EventProperty> {

	@Override
	public void write(Kryo kryo, Output output, EventProperty ep) {
		kryo.writeObject(output, ep.getId());
		kryo.writeObject(output, ep.getName());
		kryo.writeClassAndObject(output, ep.getType());
		kryo.writeObject(output, ep.getEvent());
	}

	@Override
	public EventProperty read(Kryo kryo, Input input, Class<EventProperty> type) {
		EventProperty ep=new EventProperty();
		ep.id = kryo.readObject(input, Long.class);
		ep.name = kryo.readObject(input, String.class);
		ep.type = kryo.readClassAndObject(input);
		ep.event = kryo.readObject(input, Event.class);
		return ep;
	}

}
