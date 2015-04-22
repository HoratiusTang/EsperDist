package dist.esper.core.flow.container;


import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.core.flow.stream.DerivedStream;
import dist.esper.core.flow.stream.FilterStream;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.DeepReplaceFactory;

/**
 * the container contains @FilterStream(s),
 * each of the @FitlerStream(s) holds equivalent filterExpr and view specification.
 * 
 * @see @FilterStream
 * @author tjy
 *
 */
public class FilterStreamContainer extends BaseStreamContainer{
	private static final long serialVersionUID = 8264475096776797332L;
	EventSpecification eventSpec=null;
	RawStream rawStream=null;
	AbstractBooleanExpression filterExpr=null;
	
	public FilterStreamContainer(){
	}
	
	public FilterStreamContainer(FilterStream fsl){
		this.eventSpec=(EventSpecification)cloner.deepClone(fsl.getEventSpec());
		this.rawStream=fsl.getRawStream();
		this.filterExpr=(AbstractBooleanExpression)cloner.deepClone(fsl.getFilterExpr());
		this.setWindowTimeUS(fsl.getWindowTimeUS());
		this.setWorkerId(fsl.getWorkerId());
	}

	public EventSpecification getEventSpec() {
		return eventSpec;
	}

	public void setEventSpec(EventSpecification eventSpec) {
		this.eventSpec = eventSpec;
	}

	public RawStream getRawStream() {
		return rawStream;
	}

	public void setRawStream(RawStream rawStream) {
		this.rawStream = rawStream;
	}

	public AbstractBooleanExpression getFilterExpr() {
		return filterExpr;
	}

	public void setFilterExpr(AbstractBooleanExpression filterExpr) {
		this.filterExpr = filterExpr;
	}
	
//	@Override
//	public String getWindowTimeViewSpecString(){
//		return String.format(".win:time(%d msec)", 60*1000);
//	}

	@Override
	public Stream getUpStreamByEventName(
			String childEventName) {
		if(rawStream.getInternalCompositeEvent().getName().equals(childEventName)){
			return rawStream;
		}
		return null;
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		ContainerStringlizer.toStringBuilder(this, sw, indent);
	}

	@Override
	public void merge(DerivedStream pcsl,
			Map<EventAlias, EventAlias> fscTofslMap,
			BooleanExpressionComparisonResult cr) {
		super.merge(pcsl, fscTofslMap, cr);
		DeepReplaceFactory replacer=new DeepReplaceFactory(fscTofslMap);
		mergeFitlerList(replacer);
	}
	
	protected void mergeFitlerList(DeepReplaceFactory replacer){
		replacer.deepReplace(filterExpr);
		replacer.deepReplace(eventSpec);
	}

	@Override
	public int getBaseStreamCount() {
		return 1;
	}

	@Override
	public int getRawStreamCount() {
		return 1;
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

	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		eaSet.add(this.eventSpec.getEventAlias());
	}

	@Override
	public Stream[] getUpStreams() {		
		return new Stream[]{this.rawStream};
	}

	@Override
	public AbstractBooleanExpression getOwnCondition() {
		return filterExpr;
	}

	@Override
	public void dumpAllUpStreamContainers(List<StreamContainer> dscList) {
		dscList.add(this);
	}
}
