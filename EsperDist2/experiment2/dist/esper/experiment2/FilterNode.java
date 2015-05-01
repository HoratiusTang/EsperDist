package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.epl.expr.Value;

public class FilterNode extends Node {
	FilterEventPropOpType type;
	Value val;
	public FilterNode() {
		super();
	}
	
	public FilterNode(FilterEventPropOpType type) {
		super();
		this.type = type;
	}
	
	@Override
	public String toString(){
		return String.format("FN(%d-%d-[%d,%d,%d,%d])", id, tag, type.eventType, type.propType, type.opType, type.windowType);
	}
}
