package dist.esper.experiment2;

import java.util.*;

import dist.esper.core.util.NumberFormatter;
import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.epl.expr.Value;

public class FilterNode extends Node {
	FilterEventPropOpType filterType;
	Number value;
	public FilterNode() {
		super();
	}
	
	public FilterNode(FilterEventPropOpType type) {
		super();
		this.filterType = type;
	}	
	
	public Number getValue() {
		return value;
	}

	public void setValue(Number value) {
		this.value = value;
	}

	public FilterEventPropOpType getFilterType() {
		return filterType;
	}

	public void setFilterType(FilterEventPropOpType filterType) {
		this.filterType = filterType;
	}

	@Override
	public String toString(){
		return String.format("FN(%d-%d-[%d,%d,%d,%d,%s])", id, tag, 
				filterType.eventType, filterType.propType, 
				filterType.opType, filterType.windowType,
				value==null?"null":NumberFormatter.format(value));
	}
}
