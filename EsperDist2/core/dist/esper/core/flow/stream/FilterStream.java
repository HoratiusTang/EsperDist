package dist.esper.core.flow.stream;


import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.EventSpecification;

/**
 * the stream presents a filter stream in 'FROM' clause,
 * e.g. 'A(id>5).win:time(5 hour)',
 * its up-stream is a @RawStream.
 * 
 * @see @FilterNode
 * @author tjy
 *
 */
public class FilterStream extends BaseStream{
	private static final long serialVersionUID = 7932210726216415850L;
	EventSpecification eventSpec=null;
	RawStream rawStream=null;
	AbstractBooleanExpression filterExpr=null;
	
	public FilterStream(){
		super();
	}
	
	public FilterStream(EventSpecification eventSpec,
			AbstractBooleanExpression filterExpr) {
		super();
		this.eventSpec = eventSpec;
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
	public void setFilterExpr(AbstractBooleanExpression filterExpr) {
		this.filterExpr = filterExpr;
	}
	public RawStream getRawStream() {
		return rawStream;
	}
	public void setRawStream(RawStream rawStream) {
		this.rawStream = rawStream;
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		StreamStringlizer.toStringBuilder(this, sw, indent);
	}

//	@Override
//	public Stream getChildStreamLocationByEventName(
//			String childEventName) {
//		if(rawStream.getInternalCompositeEvent().getName().equals(childEventName)){
//			return rawStream;
//		}
//		return null;
//	}

	@Override
	public int getBaseStreamCount() {
		return 1;
	}

	@Override
	public int getRawStreamCount() {
		return 1;
	}
	
	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		eaSet.add(this.getEventSpec().getEventAlias());
	}

	@Override
	public boolean tryAddResultElement(EventOrPropertySpecification eps) {
		if(eps.getEventAlias()==eventSpec.getEventAlias()){
			addNewResultElementIfNotExists(eps);
			return true;
		}
		return false;
	}

	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		return;
	}

	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> ownCondList) {
		if(this.filterExpr!=null){
			filterExpr.dumpConjunctionExpressions(ownCondList);
		}
	}
}
