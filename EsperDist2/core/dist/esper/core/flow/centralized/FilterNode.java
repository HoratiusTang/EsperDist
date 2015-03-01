package dist.esper.core.flow.centralized;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.FilterStreamSpecification;
import dist.esper.util.StringUtil;

/**
 * the node presents a filter stream in 'FROM' clause,
 * e.g. 'A(id>5).win:time(5 hour)'
 * 
 * @author tjy
 *
 */
public class FilterNode extends BaseNode {
	EventSpecification eventSpec=null;
	AbstractBooleanExpression filterExpr=null;	
		
	public FilterNode(EventAlias eventAlias){
		super();
		this.eventSpec=new EventSpecification(eventAlias, eventAlias);
		this.optionalStreamName=eventAlias.getEventAsName();
		this.eplId=eventAlias.getEplId();
	}
	
	public FilterNode(FilterNode fsn){
		//FIXME: shallow or deep?
		this.eplId=fsn.getEplId();
		this.eventSpec=fsn.eventSpec;
		this.filterExpr=fsn.filterExpr;
		this.viewSpecs=fsn.viewSpecs;
		this.optionalStreamName=fsn.optionalStreamName;
		this.resultElementList=fsn.resultElementList;
	}
	
	public void setFilterExprssion(AbstractBooleanExpression filterExpr){
		this.filterExpr = filterExpr;
	}

	public EventSpecification getEventSpec() {
		return eventSpec;
	}

	public void setEventSpec(EventSpecification eventSpec) {
		this.eventSpec = eventSpec;
	}

	public AbstractBooleanExpression getFilterExpr() {
		return filterExpr;
	}

	@Override
	public void dumpSelectedEventAliases(Set<EventAlias> eaSet) {
		eaSet.add(eventSpec.getEventAlias());
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		NodeStringlizer.toStringBuilder(this, sw, indent);
	}
}
