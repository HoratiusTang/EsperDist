package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;

public class FilterNode extends Node {
	public int eventType;
	public int propType;
	public int opType;
	//public OperatorTypeEnum opType;
	public FilterNode() {
		super();
	}
	public FilterNode(int eventType, int propType, int opType) {
		super();
		this.eventType = eventType;
		this.propType = propType;
		this.opType = opType;
	}
}
