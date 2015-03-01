package dist.esper.event;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class EventRegistry implements Serializable{
	private static final long serialVersionUID = 6685488304512558784L;
	Map<String,Event> eventMap=new ConcurrentSkipListMap<String,Event>();
	
	public void registEvent(Event event){
		//System.out.format("** regist Event: %s\n", event.toString());
		eventMap.put(event.getName(),event);
	}
	
	public Event resolveEvent(String eventTypeName){
		Event event=eventMap.get(eventTypeName);
		return event;
	}
	
	public Collection<Event> getRegistedEvents(){
		return eventMap.values();
	}

	public Map<String, Event> getEventMap() {
		return eventMap;
	}

	public void setEventMap(Map<String, Event> eventMap) {
		this.eventMap = eventMap;
	}
	
}
