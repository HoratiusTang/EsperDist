package dist.esper.epl.expr;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import dist.esper.event.Event;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.EventAliasJsonSerializer.class)
public class EventAlias implements Serializable{
	private static final long serialVersionUID = -8481924764787282294L;
	long eplId=-1;
	long id=-1;
	Event event=null;
	String eventAsName=null;
	transient Object source=null;
	//boolean isArray=false;
	//int index=-1;
	transient static AtomicLong eventAliasUID=new AtomicLong(0L);
	
	public EventAlias() {
		super();
	}
	public EventAlias(Event event){
		this.event=event;
	}
	public EventAlias(long eplId, Event event, Object source) {
		this.eplId=eplId;
		this.event=event;
		this.source=source;
		this.id=eventAliasUID.getAndIncrement();
	}
	
	public EventAlias(long eplId, Event event){
		this.eplId=eplId;
		this.event=event;
		this.id=eventAliasUID.getAndIncrement();
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

	public String getEventAsName() {
		return eventAsName;
	}

	public void setEventAsName(String eventAsName) {
		this.eventAsName = eventAsName;
	}
//	public boolean isArray() {
//		return isArray;
//	}
//
//	public void setArray(boolean isArray) {
//		this.isArray = isArray;
//	}

//	public int getIndex() {
//		return index;
//	}
//
//	public void setIndex(int index) {
//		this.index = index;
//	}

	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}

	public Object getSource() {
		return source;
	}

	public void setSource(Object source) {
		this.source = source;
	}
	
	@Override
	public String toString(){
		if(eventAsName!=null){
			return eventAsName;
		}
		else{
			return event.getName();			
		}
	}

	//@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(toString());
	}

	//@Override
//	public boolean resolve(StatementSementicWrapper ssw,Object param) {
//		// TODO Auto-generated method stub
//		return true;
//	}
	
	@Override
	public int hashCode(){
		String s=event.getName();
		if(this.eventAsName!=null){
			s+=this.eventAsName;
		}
		return s.hashCode();
	}
	
	public boolean equalsIngoreAlias(EventAlias eventAlias){
		if(this.getEvent().equals(eventAlias.getEvent())){
			return true;
		}
		return false;
	}

	@Override
	public boolean equals(Object obj){
		if(obj instanceof EventAlias){
			EventAlias ea=(EventAlias)obj;
			if(event.equals(ea.getEvent())){
				if((eventAsName==null && ea.eventAsName==null) || eventAsName.equals(ea.eventAsName)){
					if(source==ea.source){
						return true;
					}
				}
			}
		}
		return false;
	}

	//@Override
	public int eigenCode() {
		return event.getName().hashCode();
	}
}
