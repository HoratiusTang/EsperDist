package dist.esper.core.flow.container;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.core.flow.stream.FilterDelayedStream;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.CompositeExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.epl.expr.util.DeepReplaceFactory;

/**
 * the container contains @FilterDelayedStream(s),
 * each of the @FitlerDelayedStream(s) holds equivalent extraFilterCondList and agent.
 * 
 * @FilterDelayedStream
 * @author tjy
 *
 */
public class FilterDelayedStreamContainer extends
		FilterStreamContainer {
	private static final long serialVersionUID = 4738052660811566228L;
	FilterStreamContainer agent;
	List<AbstractBooleanExpression> extraFilterCondList=new ArrayList<AbstractBooleanExpression>(2);
	
	public FilterDelayedStreamContainer(){
	}
	public FilterDelayedStreamContainer(
			FilterDelayedStream fcsl,  FilterStreamContainer agent){
		this.setAgent(agent);
		for(AbstractBooleanExpression extraJoinCond: fcsl.getExtraFilterCondList()){
			extraFilterCondList.add((AbstractBooleanExpression)cloner.deepClone(extraJoinCond));
		}
		//copy Result Element List
		for(SelectClauseExpressionElement agentSe: agent.getResultElementList()){
			this.addResultElement(new SelectExpressionElementContainer(agentSe));
		}
		this.setWindowTimeUS(fcsl.getWindowTimeUS());
		this.setWorkerId(fcsl.getWorkerId());
	}
	
	@Override
	public EventSpecification getEventSpec() {
		return agent.getEventSpec();
	}

	@Override
	public void setEventSpec(EventSpecification eventSpec) {
		agent.setEventSpec(eventSpec);
	}

	@Override
	public RawStream getRawStream() {
		return agent.getRawStream();
	}

	@Override
	public void setRawStream(RawStream rawStreamLocation) {
		agent.setRawStream(rawStreamLocation);
	}

	@Override
	public AbstractBooleanExpression getFilterExpr() {
		return agent.getFilterExpr(); 
	}

	@Override
	public void setFilterExpr(AbstractBooleanExpression filterExpr) {
		agent.setFilterExpr(filterExpr);
	}

	public FilterStreamContainer getAgent() {
		return agent;
	}

	public void setAgent(FilterStreamContainer agent) {
		this.agent = agent;
		if(agent!=null){
			this.agent.addDownContainerId(this.id);
		}
	}
	
	public List<AbstractBooleanExpression> getExtraFilterCondList() {
		return extraFilterCondList;
	}

	public void setExtraFilterCondList(
			List<AbstractBooleanExpression> extraFilterCondList) {
		this.extraFilterCondList = extraFilterCondList;
	}
	
	@Override
	public Stream[] getUpStreams(){
		return new Stream[]{agent};
	}

	@Override
	public Stream getUpStreamByEventName(
			String childEventName) {		
		return this.agent;
	}
	
	@Override
	public int getLevel() {
		return agent.getLevel()+1;
	}
	
	/**
	public void merge(ProcessedStreamLocation pcsl,
			Map<EventAlias, EventAlias> fcscTofslMap,
			BooleanExpressionComparisonResult cr) {
		FilterStreamLocation fsl=(FilterStreamLocation)pcsl;
		DeepReplaceFactory replacer=new DeepReplaceFactory(fcscTofslMap);
		mergeFitlerList(replacer);
		mergeResultList(fsl, replacer);
		
		Map<EventAlias, EventAlias> fslTofcscMap=CollectionUtils.reverse(fcscTofslMap);
		if(cr.getTotalState()==State.EQUAL){
			mergeMeta(fsl);
			this.reusedLocationMapComparisonResultList.add(
					new LocationAndMapAndBoolComparisonResult(fsl, fslTofcscMap, cr));
		}
		else if(cr.getTotalState()==State.COMPATIBLE){
			this.compatedLocationMapComparisonResultList.add(
					new LocationAndMapAndBoolComparisonResult(fsl, fslTofcscMap, cr));
		}
	}
	**/
	
	protected void mergeFitlerList(DeepReplaceFactory replacer){
		for(AbstractBooleanExpression extraFilterCond: extraFilterCondList){
			replacer.deepReplace(extraFilterCond);
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		ContainerStringlizer.toStringBuilder(this, sw, indent);
	}
	
	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		agent.dumpAllBooleanExpressions(childrenCondList);//FIXME
	}

	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> ownCondList) {
		for(AbstractBooleanExpression extraFilterCond: this.extraFilterCondList){
			extraFilterCond.dumpConjunctionExpressions(ownCondList);
		}
	}
	
	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		agent.dumpEventAlias(eaSet);
	}
	
	@Override
	public ViewSpecification[] getViewSpecs() {
		return agent.getViewSpecs();
	}
	
	@Override
	public AbstractBooleanExpression getOwnCondition() {
		if(this.extraFilterCondList==null || this.extraFilterCondList.size()<1){
			return extraFilterCondList.get(0);
		}
		else if(this.extraFilterCondList.size()==1){
			return this.extraFilterCondList.get(0);
		}
		return new CompositeExpression(RelationTypeEnum.AND, this.extraFilterCondList);
	}
}
