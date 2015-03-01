package dist.esper.external.event;

import java.util.*;

import dist.esper.event.Event;

public class EventInstanceGenerator {	
	Map<String,FieldGenerator> fgMap=new HashMap<String,FieldGenerator>();
	Event event;

	public EventInstanceGenerator(Event event, Map<String,FieldGenerator> fgMap) {
		super();
		this.fgMap = fgMap;
		this.event = event;
	}
	
	public TreeMap<String,Object> nextEventInstance(){
		TreeMap<String,Object> ins=new TreeMap<String,Object>();
		
		for(Map.Entry<String, FieldGenerator> entry: fgMap.entrySet()){
			ins.put(entry.getKey(), entry.getValue().next());
		}
		return ins;
	}

	public Map<String, FieldGenerator> getFgMap() {
		return fgMap;
	}

	public Event getEvent() {
		return event;
	}
	
}
