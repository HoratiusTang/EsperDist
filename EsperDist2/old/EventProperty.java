package dist.esper.event;



import dist.esper.epl.expr.DataTypeEnum;
import dist.esper.epl.sementic.StatementSementicWrapper;

@Deprecated
public class EventProperty{
	public Event event=null;
	public String name="";
	public DataTypeEnum type=DataTypeEnum.NONE;
	
	public EventProperty(Event event, String name, DataTypeEnum type) {
		super();
		this.event = event;
		this.name = name;
		this.type = type;
	}

	public EventProperty(String name, DataTypeEnum type) {
		super();
		this.name = name;
		this.type = type;
	}
	
	public boolean isArray(){
		return type.isArray();
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

	public void setName(String name) {
		this.name = name;
	}

	public DataTypeEnum getType() {
		return type;
	}

	public void setType(DataTypeEnum type) {
		this.type = type;
	}
	
	@Override
	public String toString(){
		return this.name+":"+this.type.toString();
	}

	//@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(this.toString());
	}
	
	public String getFullName(){
		return event.getFullName()+"."+name;
	}

//	@Override
//	public boolean resolve(StatementSementicWrapper ssw, Object param) {
//		// TODO Auto-generated method stub
//		return true;
//	}
	
	
}
