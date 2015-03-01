package dist.esper.epl.expr;


import java.util.*;

import dist.esper.util.ReflectionFactory;
import dist.esper.event.Event;

@Deprecated
public class EventRegistry {
	HashMap<String,Event> fullNameMap=new HashMap<String,Event>();
	HashMap<String,Event> nameMap=new HashMap<String,Event>();
	
	public EventRegistry(){
	}
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		String delimiter="";
		for(Map.Entry<String,Event> entry: fullNameMap.entrySet()){
			sw.append(delimiter);
			sw.append(entry.getKey());
			sw.append("=");
			entry.getValue().toStringBuilder(sw);
			delimiter=", ";
		}
		sw.append("\n");
		delimiter="";
		for(Map.Entry<String,Event> entry: nameMap.entrySet()){
			sw.append(delimiter);
			sw.append(entry.getKey());
			sw.append("=");
			entry.getValue().toStringBuilder(sw);
			delimiter=", ";
		}
		return sw.toString();
	}
	
	public void registEvent(Event event){
		//fullNameMap.put(event.getFullName(),event);
		nameMap.put(event.getName(),event);
	}
	
	public void registClassByName(String className){
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
			registClass(clazz);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void registPackageByName(String packageName){
		List<Class<?>> clazzList=ReflectionFactory.getPackageClasses(packageName);
		for(Class<?> clazz: clazzList){
			registClass(clazz);
		}
	}
	
	public void registClass(Class<?> clazz){
		if(!clazz.isInterface()){
			//Event event=Event.Factory.make(clazz);		
			//registEvent(event);
		}
	}
	
	public Event resolveEvent(String eventTypeName){
		Event event=null;
		if(eventTypeName.contains(".")){
			event=fullNameMap.get(eventTypeName);
		}
		else{
			event=nameMap.get(eventTypeName);
		}	
		return event;
	}
}
