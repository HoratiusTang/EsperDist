package dist.esper.external.event;

import java.util.*;

import dist.esper.event.Event;

public class EventInstanceGenerator {	
	Map<String,FieldGenerator> fgMap=new HashMap<String,FieldGenerator>();
	Event event;

	public EventInstanceGenerator() {
		this("");
	}
	
	public EventInstanceGenerator(String eventName){
		event=new Event(eventName);
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
	
	public void setEventName(String eventName){
		event.setName(eventName);	
	}
	
	public String getEventName(){
		return event.getName();
	}
	
}
