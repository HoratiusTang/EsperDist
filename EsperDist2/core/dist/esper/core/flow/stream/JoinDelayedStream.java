package dist.esper.core.flow.stream;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.AbstractResultExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.BooleanExpressionComparisonPair;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;
import dist.esper.epl.expr.util.EventOrPropertySpecReferenceDumper;
import dist.esper.event.Event;

/**
 * the stream which performs a delayed filter/join,
 * e.g. performs (a.age>10 and b.time>a.time) after that the (a.id>5) is performed by its up-stream. 
 * its up-stream is a @JoinStream.
 * 
 * @see @JoinStream
 * @author tjy
 *
 */
public class JoinDelayedStream extends JoinStream{	
	private static final long serialVersionUID = 2889340693877506426L;
	JoinStream agent;
	List<AbstractBooleanExpression> extraJoinCondList=new ArrayList<AbstractBooleanExpression>(2);
	List<AbstractBooleanExpression> extraChildCondList=new ArrayList<AbstractBooleanExpression>(4);
	
	public JoinDelayedStream(){		
	}
	public JoinDelayedStream(
			JoinStream agent, 
			//JoinStreamLocationContainer jsc,
			//Map<EventAlias,EventAlias> eaMap, //jsc->jsl
			BooleanExpressionComparisonResult cr){		
		this.agent=agent;
		this.eplId=agent.getEplId();//ATT
		for(BooleanExpressionComparisonPair jccp: cr.getOwnPairList()){
			if(jccp.getState()==State.IMPLYING || jccp.getState()==State.SURPLUS){
				extraJoinCondList.add((AbstractBooleanExpression)cloner.deepClone(jccp.getFirst()));
			}
		}
		for(BooleanExpressionComparisonPair cccp: cr.getChildPairList()){
			if(cccp.getState()==State.IMPLYING || cccp.getState()==State.SURPLUS){
				extraChildCondList.add((AbstractBooleanExpression)cloner.deepClone(cccp.getFirst()));
			}
		}
		for(SelectClauseExpressionElement se: agent.getResultElementList()){
			this.addResultElement(new SelectClauseExpressionElement(
							(AbstractResultExpression)cloner.deepClone(se.getSelectExpr())));
		}
		this.setWindowTimeUS(agent.getWindowTimeUS());
	}
	public Set<EventOrPropertySpecification> dumpExtraConditionEventOrPropertySpecReferences(){
		Set<EventOrPropertySpecification> epsSet=new HashSet<EventOrPropertySpecification>();
		
		EventOrPropertySpecReferenceDumper.dump(extraJoinCondList, epsSet);
		EventOrPropertySpecReferenceDumper.dump(extraChildCondList, epsSet);
		return epsSet;
	}
	public List<Stream> getUpStreamList() {
		return agent.getUpStreamList();
	}
	public Stream getUpStream(int index){
		return agent.getUpStream(index);
	}
	public int getUpStreamCount(){
		return agent.getUpStreamCount();
	}
	public JoinStream getAgent() {
		return agent;
	}
	public void setAgent(JoinStream agent) {
		this.agent = agent;
	}
	public List<AbstractBooleanExpression> getExtraJoinCondList() {
		return extraJoinCondList;
	}
	public List<AbstractBooleanExpression> getExtraChildCondList() {
		return extraChildCondList;
	}
	@Override
	public int getBaseStreamCount() {
		return agent.getBaseStreamCount();
	}

	@Override
	public int getRawStreamCount() {
		return agent.getRawStreamCount();
	}
	
	@Override
	public int getLevel() {
		return agent.getLevel()+1;
	}

//	@Override
//	public Stream getChildStreamLocationByEventName(
//			String childEventName) {
//		return agent.getChildStreamLocationByEventName(childEventName);//ATT
//	}
	
	@Override
	public void dumpEvents(Collection<Event> events) {
		agent.dumpEvents(events);
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
			List<AbstractBooleanExpression> ownCondList) {
		for(AbstractBooleanExpression extraJoinCond: this.extraJoinCondList){
			extraJoinCond.dumpConjunctionExpressions(ownCondList);
		}
		for(AbstractBooleanExpression extraChildCond: this.extraChildCondList){
			extraChildCond.dumpConjunctionExpressions(ownCondList);
		}
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
	
	public void setExtraJoinCondList(
			List<AbstractBooleanExpression> extraJoinCondList) {
		this.extraJoinCondList = extraJoinCondList;
	}
	public void setExtraChildCondList(
			List<AbstractBooleanExpression> extraChildCondList) {
		this.extraChildCondList = extraChildCondList;
	}
	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		StreamStringlizer.toStringBuilder(this, sw, indent);
	}
}
