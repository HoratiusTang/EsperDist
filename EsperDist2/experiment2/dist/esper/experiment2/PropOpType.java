package dist.esper.experiment2;

public class PropOpType {
	public int propType;
	public int opType;
	public PropOpType(int propType, int opType) {
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
		if(obj instanceof EventPropOpType){
			EventPropOpType that=(EventPropOpType)obj;
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
