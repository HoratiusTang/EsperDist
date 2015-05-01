package dist.esper.experiment2;

public class EventPropOpType{
	public int eventType;
	public int propType;
	public int opType;
	public int window;
	public EventPropOpType(int eventType, int propType, int opType, int window) {
		super();
		this.eventType = eventType;
		this.propType = propType;
		this.opType = opType;
		this.window = window;
	}
	
	@Override
	public int hashCode(){
		return eventType*1000 + propType*100 + opType*10;// + window;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof EventPropOpType){
			EventPropOpType that=(EventPropOpType)obj;
			return this.eventType == that.eventType &&
					this.propType == that.propType &&
					  this.opType == that.opType &&
					  this.window == that.window;
		}
		return false;
	}
	
	@Override
	public String toString(){
		return String.format("(%d,%d,%d,%d)", eventType, propType, opType, window);
	}
}
