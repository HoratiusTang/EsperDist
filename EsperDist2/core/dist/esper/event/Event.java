package dist.esper.event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import com.esotericsoftware.kryo.DefaultSerializer;

import dist.esper.event.io.EventSerializer;

//@JsonSerialize(using = EventJsonSerializerFactory.EventJsonSerializer.class)
@DefaultSerializer(value = EventSerializer.class)
public class Event extends TreeMap<String, Object>{	
	private static final long serialVersionUID = -2368578414816451930L;
	String name;
	List<EventProperty> propList=new ArrayList<EventProperty>(4);
	static AtomicLong UID=new AtomicLong(0L);
	long id;
	
	public Event() {
		super();
	}

	public Event(String name) {
		super();
		this.name = name;
		id=UID.getAndIncrement();
	}
	
	/**
	 * add new property
	 * @param propName the name of the property
	 * @param propType the type of the property, can be xxx.class or another MappedEvent
	 * @return
	 */
	public void addProperty(String propName, Object propType){
		EventProperty prop=new EventProperty(propName, propType);
		addProperty(prop);
	}
	
	public void addProperty(EventProperty property){
		property.setEvent(this);
		propList.add(property);
		put(property.getName(), property.getType());
	}
	
	public EventProperty getProperty(String propName){
		for(EventProperty p: propList){
			if(p.name.equals(propName)){
				return p;
			}
		}
		return null;
	}
	
	public void refresh(){
		this.clear();
		for(EventProperty p: propList){
			this.put(p.getName(), p.getType());
		}
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public void setPropList(List<EventProperty> propList) {
		this.propList = propList;
	}

	public List<EventProperty> getPropList() {
		return propList;
	}

	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw);
		return sw.toString();
	}

	//@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(this.name);
		sw.append('[');
		String delimiter="";
		for(EventProperty prop: this.getPropList()){
			sw.append(delimiter);
			prop.toStringBuilder(sw);
			delimiter=", ";
		}
		sw.append(']');
		sw.append('(');
		delimiter="";
		for(Map.Entry<String, Object> e: this.entrySet()){
			sw.append(delimiter);
			sw.append(e.getKey());
			sw.append(":");
			sw.append(e.getValue());
			delimiter=", ";
		}
		sw.append(')');
	}
	
	@Override
	public int hashCode(){
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj){
		if(obj instanceof Event){
			return this.name.equals(((Event)obj).getName());
		}
		return false;
	}
}
