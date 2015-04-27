package dist.esper.event;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.esotericsoftware.kryo.DefaultSerializer;

import dist.esper.event.io.EventPropertySerializer;

@DefaultSerializer(value = EventPropertySerializer.class)
public class EventProperty implements Serializable{	
	private static final long serialVersionUID = 5765766750126884792L;
	public Event event=null;
	public String name="";
	public Object type=null;
	public long id;
	transient static AtomicLong UID=new AtomicLong(0L);
	
	public EventProperty() {
		super();
	}

	public EventProperty(Event event, String name, Object type) {
		super();
		this.event = event;
		this.name = name;
		this.type = type;
		id=UID.getAndIncrement();
	}
	
	public EventProperty(String name, Object type) {
		super();
		this.name = name;
		this.type = type;
		id=UID.getAndIncrement();
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public Event getEvent() {
		return event;
	}

	public void setEvent(Event event) {
		this.event = event;
	}

	public String getName() {
		return name;
	}
	
	public String fullName(){
		return event.name+"."+this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Object getType() {
		return type;
	}

	public void setType(Object type) {
		this.type = type;
	}
	
	public boolean isInteger(){
		String simpleName=null;
		if(type instanceof Class<?>){
			simpleName=((Class<?>)type).getSimpleName();			
		}
		else if(type instanceof String){
			simpleName=(String)type;
		}
		if(simpleName!=null){
			return simpleName.equals("byte") || 
					simpleName.equals("Byte") ||
					simpleName.equals("char") || 
					simpleName.equals("Character") ||
					simpleName.equals("short") || 
					simpleName.equals("Short") ||
					simpleName.equals("int") || 
					simpleName.equals("Integer") || 
					simpleName.equals("long") || 
					simpleName.equals("Long");
		}
		return false;
	}
	
	public boolean isFloat(){
		String simpleName=null;
		if(type instanceof Class<?>){
			simpleName=((Class<?>)type).getSimpleName();			
		}
		else if(type instanceof String){
			simpleName=(String)type;
		}
		if(simpleName!=null){
			return simpleName.equals("float") || 
					simpleName.equals("Float") || 
					simpleName.equals("double") || 
					simpleName.equals("Double");
		}
		return false;
	}
	
	public boolean isArray(){
		if(type instanceof Class<?>){
			return ((Class<?>)type).isArray();
		}
		else if(type instanceof String){
			return ((String)type).endsWith("[]");
		}
		//if the type is Event, can't be array
		return false;
	}
	
	public boolean isString(){
		if(type instanceof Class<?>){
			if(String.class.equals(type)){
				return true;
			}
		}
		else if(type instanceof String){
			if(type.equals("String") || type.equals("java.lang.String")){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * @return the Class representing the component type of an array. 
	 * If this class does not represent an array class this method returns null.  
	 */
	public Object getComponentType(){
		if(type instanceof Class<?>){
			return ((Class<?>)type).getComponentType();
		}
		else if(type instanceof Event){
			return type;
		}
		else if(type instanceof String){
			String str=(String)type;
			if(str.endsWith("[]")){
				return str.substring(0, str.length()-2);
			}
			else{
				return type;
			}
		}
		return null;
	}
	
	@Override
	public int hashCode(){
		return this.fullName().hashCode();
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof EventProperty){
			EventProperty prop=(EventProperty)obj;
			if(this.getEvent().equals(prop.getEvent()) && 
					this.name.equals(prop.name)){
				return true;
			}
		}
		return false;
	}
	
	@Override
	public String toString(){
		if(type instanceof Class<?>){
			return name+":"+((Class<?>)type).getSimpleName();
		}
		else if(type instanceof Event){
			Event me=(Event)type;
			return name+":"+me.getName();
		}
		else{
			return name+":"+type.toString();
		}
	}
	
	public String getTypeSimpleName(){
		if(type instanceof Class<?>){
			return ((Class<?>)type).getSimpleName();
		}
		else if(type instanceof Event){
			//return ((Event)type).getClass().getSimpleName();
			return ((Event)type).getName();
		}
		else{
			return type.toString();
		}
	}
	
	public void toStringBuilder(StringBuilder sw) {
		sw.append(this.toString());
	}
}
