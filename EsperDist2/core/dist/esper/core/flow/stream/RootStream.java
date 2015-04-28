package dist.esper.core.flow.stream;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.event.Event;

/**
 * the final stream in @StreamTree.
 * its resultElementList is the final output.
 * 
 * @see @RootNode
 * @author tjy
 *
 */
public class RootStream extends DerivedStream{
	private static final long serialVersionUID = 7057523253168113857L;
	Stream upStream;
	
	/**if the child is a PatternNode, the whereExprList may not be empty*/
	List<AbstractBooleanExpression> whereExprList=new ArrayList<AbstractBooleanExpression>(2);

	public RootStream() {
		super();
	}
	
	public RootStream(Stream child) {
		super();
		this.upStream = child;
	}

	public Stream getUpStream() {
		return upStream;
	}

	public void setUpStream(Stream child) {
		this.upStream = child;
	}
	
	public void addWhereExprssion(AbstractBooleanExpression whereExpr){
		whereExprList.add(whereExpr);
	}
	
	public List<AbstractBooleanExpression> getWhereExprList() {
		return whereExprList;
	}
	
	public void setWhereExprList(List<AbstractBooleanExpression> whereExprList) {
		this.whereExprList.addAll(whereExprList);
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		StreamStringlizer.toStringBuilder(this, sw, indent);
	}

//	@Override
//	public Stream getChildStreamLocationByEventName(
//			String childEventName) {
//		if(upStream.getInternalCompositeEvent().getName().equals(childEventName)){
//			return upStream;
//		}
//		return null;
//	}

	@Override
	public int getBaseStreamCount() {
		return upStream.getBaseStreamCount();
	}

	@Override
	public int getRawStreamCount() {
		return upStream.getRawStreamCount();
	}
	@Override
	public boolean tryAddResultElement(EventOrPropertySpecification eps) {
		if(upStream instanceof DerivedStream){
			boolean childAdded=((DerivedStream)upStream).tryAddResultElement(eps);
			if(childAdded){
				addNewResultElementIfNotExists(eps);
				return true;
			}
		}
		return false;
	}
	
	@Override
	public void dumpEvents(Collection<Event> events) {
		((DerivedStream)upStream).dumpEvents(events);
	}

	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		((DerivedStream)upStream).dumpEventAlias(eaSet);
	}

	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		((DerivedStream)upStream).dumpAllBooleanExpressions(childrenCondList);
	}

	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> ownCondList) {
		ownCondList.addAll(whereExprList);
	}

	@Override
	public ViewSpecification[] getViewSpecs() {
		// TODO Auto-generated method stub
		return ((DerivedStream)upStream).getViewSpecs();
	}

	@Override
	public int getLevel() {
		return upStream.getLevel()+1;
	}
}
