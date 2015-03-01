package dist.esper.epl.sementic;

import java.util.*;

import dist.esper.epl.expr.*;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;
import dist.esper.event.EventRegistry;

@Deprecated
public class StatementSementicWrapper {
	public long eplId=-1;
	public HashMap<String,EventAlias> eventAliasMap=new HashMap<String,EventAlias>(8);
	public HashMap<String,EventPropertySpecification> propAliasMap=new HashMap<String,EventPropertySpecification>(8);
	public HashMap<String,EventAlias> noAliasFilterEventAliasNameMap=new HashMap<String,EventAlias>(8);
	//public HashMap<String,EventAlias> noAliasFilterEventAliasFullNameMap=new HashMap<String,EventAlias>(8);
	public EventRegistry eventRegistry=null;
	
	public StatementSementicWrapper(long eplId, EventRegistry eventRegistry) {
		super();
		this.eplId = eplId;
		this.eventRegistry = eventRegistry;
	}
	
	public EventAlias searchProperty(String propName){
		//for(Map.Entry<String,EventAlias> entry: noAliasFilterEventAliasFullNameMap.entrySet()){
		for(Map.Entry<String,EventAlias> entry: noAliasFilterEventAliasNameMap.entrySet()){
			Event event=entry.getValue().getEvent();
			EventProperty eventProperty=event.getProperty(propName);
			if(eventProperty!=null){
				return entry.getValue();
			}
		}
		return null;
	}
	
	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}

	public EventAlias searchEventAlias(String name){
		return eventAliasMap.get(name);
	}
	
	public EventAlias searchEventName(String name){
		EventAlias eventAlias=null;
//		if(name.contains(".")){
//			eventAlias=noAliasFilterEventAliasFullNameMap.get(name);
//		}
//		else{
			eventAlias=noAliasFilterEventAliasNameMap.get(name);
//		}
		return eventAlias;
	}
}
