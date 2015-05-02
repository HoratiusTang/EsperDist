package dist.esper.external.event;

import java.util.*;

import dist.esper.event.Event;
import dist.esper.event.EventProperty;

public class EventInstanceGenerator {
	String categoryName;//different event may share the same prototype(category).
	Map<String,FieldGenerator> fgMap=new HashMap<String,FieldGenerator>();
	Event event;//event.name might be empty

	public EventInstanceGenerator() {
		this("DEFUALT_EVENT_CATEGORY", "");
	}
	
	public EventInstanceGenerator(String categoryName) {
		this(categoryName, "");
	}
	
	public EventInstanceGenerator(String categoryName, String eventName){
		this.categoryName = categoryName;
		this.event=new Event(eventName);
	}
	
	public void addProperty(String propName, FieldGenerator fgen){
		fgMap.put(propName, fgen);
		event.addProperty(propName, fgen.getFieldClassType());
	}
	
	public TreeMap<String,Object> nextEventInstance(){
		TreeMap<String,Object> ins=new TreeMap<String,Object>();
		
		for(Map.Entry<String, FieldGenerator> entry: fgMap.entrySet()){
			ins.put(entry.getKey(), entry.getValue().next());
		}
		return ins;
	}

	public Map<String, FieldGenerator> getFieldGeneratorMap() {
		return fgMap;
	}

	public Event getEvent() {
		return event;
	}
	
	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public void setEventName(String eventName){
		event.setName(eventName);	
	}
	
	public String getEventName(){
		return event.getName();
	}
	
	public String getPropertyNameByIndex(int propIndex){
		EventProperty prop=event.getPropList().get(propIndex);
		return prop.getName();
	}
	
	public FieldGenerator getFieldGeneratorByIndex(int propIndex){
		String propName=getPropertyNameByIndex(propIndex);
		return fgMap.get(propName);
	}
	
}
