package dist.esper.experiment2;

public class JoinPropOpType {
	int propType;
	int opType;
	public JoinPropOpType(int propType, int opType) {
		super();
		
		this.propType = propType;
		this.opType = opType;
	}
	
	@Override
	public int hashCode(){
		return propType*10 + opType;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof FilterEventPropOpType){
			FilterEventPropOpType that=(FilterEventPropOpType)obj;
			return this.propType == that.propType &&
					  this.opType == that.opType;
		}
		return false;
	}
	
	@Override
	public String toString(){
		return String.format("(%d,%d)", propType, opType);
	}
}
