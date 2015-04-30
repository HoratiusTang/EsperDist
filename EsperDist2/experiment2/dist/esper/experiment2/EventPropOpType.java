package dist.esper.experiment2;

class EventPropOpType{
	public int eventType;
	public int propType;
	public int opType;
	public EventPropOpType(int eventType, int propType, int opType) {
		super();
		this.eventType = eventType;
		this.propType = propType;
		this.opType = opType;
	}
	
	@Override
	public int hashCode(){
		return eventType*100 + propType*10 + opType;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof EventPropOpType){
			EventPropOpType that=(EventPropOpType)obj;
			return this.eventType == that.eventType &&
					this.propType == that.propType &&
					  this.opType == that.opType;
		}
		return false;
	}
}
