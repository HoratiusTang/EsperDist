package dist.esper.experiment2;

public class FilterEventPropOpType{
	public int eventType;
	public int propType;
	public int opType;
	public int windowType;
	public FilterEventPropOpType(int eventType, int propType, int opType, int windowType) {
		super();
		this.eventType = eventType;
		this.propType = propType;
		this.opType = opType;
		this.windowType = windowType;
	}
	
	@Override
	public int hashCode(){
		return eventType*1000 + propType*100 + opType*10;// + window;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof FilterEventPropOpType){
			FilterEventPropOpType that=(FilterEventPropOpType)obj;
			return this.eventType == that.eventType &&
					this.propType == that.propType &&
					  this.opType == that.opType &&
					  this.windowType == that.windowType;
		}
		return false;
	}
	
	@Override
	public String toString(){
		return String.format("(%d,%d,%d,%d)", eventType, propType, opType, windowType);
	}
}
