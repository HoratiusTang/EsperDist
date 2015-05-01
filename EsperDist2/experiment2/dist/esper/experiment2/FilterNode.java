package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;

public class FilterNode extends Node {
	EventPropOpType type;
	public FilterNode() {
		super();
	}
	
	public FilterNode(EventPropOpType type) {
		super();
		this.type = type;
	}
	
	@Override
	public String toString(){
		return String.format("FN(%d-%d-[%d,%d,%d])", id, tag, type.eventType, type.propType, type.opType);
	}
}
