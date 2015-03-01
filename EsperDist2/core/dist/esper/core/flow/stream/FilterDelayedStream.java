package dist.esper.core.flow.stream;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.AbstractResultExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.BooleanExpressionComparisonPair;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;
import dist.esper.epl.expr.util.EventOrPropertySpecReferenceDumper;

/**
 * the stream which performs a delayed-filter, 
 * e.g. performs (a.id>10) after that the (a.id>5) is performed by its up-stream,
 * its up-stream is a @FilterStream.
 * 
 * @see @FilterStream
 * @author tjy
 *
 */
public class FilterDelayedStream extends FilterStream {
	private static final long serialVersionUID = -4047637829287787593L;
	FilterStream agent;
	List<AbstractBooleanExpression> extraFilterCondList=new ArrayList<AbstractBooleanExpression>(2);
	
	public FilterDelayedStream(){
	}
	public FilterDelayedStream(FilterStream agent,
			//Map<EventAlias,EventAlias> eaMap,
			BooleanExpressionComparisonResult cr){
		this.agent=agent;
		this.eplId=agent.getEplId();//ATT
		for(BooleanExpressionComparisonPair fccp: cr.getOwnPairList()){
			if(fccp.getState()==State.IMPLYING || fccp.getState()==State.SURPLUS){
				extraFilterCondList.add((AbstractBooleanExpression)cloner.deepClone(fccp.getFirst()));
			}
		}
		for(SelectClauseExpressionElement se: agent.getResultElementList()){
			this.addResultElement(new SelectClauseExpressionElement(
							(AbstractResultExpression)cloner.deepClone(se.getSelectExpr())));
		}
		this.setWindowTimeUS(agent.getWindowTimeUS());
	}
	
	public EventSpecification getEventSpec() {
		return agent.getEventSpec();
	}
	public void setEventSpec(EventSpecification eventSpec) {
		agent.setEventSpec(eventSpec);
	}
	public AbstractBooleanExpression getFilterExpr() {
		return agent.getFilterExpr();
	}
	public void setFilterExpr(AbstractBooleanExpression filterExpr) {
		agent.setFilterExpr(filterExpr);
	}
	public RawStream getRawStream() {
		return agent.getRawStream();
	}
	public void setRawStream(RawStream rawStream) {
		agent.setRawStream(rawStream);
	}	
//	@Override
//	public Stream getChildStreamLocationByEventName(
//			String childEventName) {
//		return agent.getChildStreamLocationByEventName(childEventName);//ATT
//	}
	public FilterStream getAgent() {
		return agent;
	}
	public void setAgent(FilterStream agent) {
		this.agent = agent;
	}
	public void setExtraFilterCondList(
			List<AbstractBooleanExpression> extraFilterCondList) {
		this.extraFilterCondList = extraFilterCondList;
	}
	public List<AbstractBooleanExpression> getExtraFilterCondList() {
		return extraFilterCondList;
	}
	
	public int getLevel(){
		return 2;
	}
	
	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		agent.dumpEventAlias(eaSet);
	}
	
	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		agent.dumpAllBooleanExpressions(childrenCondList);//FIXME
	}

	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> curCondList) {
		for(AbstractBooleanExpression extraFilterCond: this.extraFilterCondList){
			extraFilterCond.dumpConjunctionExpressions(curCondList);
		}
	}
	
	public Set<EventOrPropertySpecification> dumpExtraConditionEventOrPropertySpecReferences(){
		Set<EventOrPropertySpecification> epsSet=EventOrPropertySpecReferenceDumper.dump(this.extraFilterCondList, null);
		return epsSet;
	}
	
	@Override
	public boolean tryAddResultElement(EventOrPropertySpecification eps) {
		boolean agentAdded=agent.tryAddResultElement(eps);
		if(agentAdded){
			addNewResultElementIfNotExists(eps);
			return true;
		}
		return false;
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		StreamStringlizer.toStringBuilder(this, sw, indent);
	}
}
