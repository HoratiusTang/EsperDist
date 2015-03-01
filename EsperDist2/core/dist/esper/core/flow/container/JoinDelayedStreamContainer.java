package dist.esper.core.flow.container;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.core.flow.stream.JoinDelayedStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.CompositeExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.epl.expr.util.DeepReplaceFactory;

/**
 * the container contains @JoinDelayedStream(s),
 * each of the @JoinDelayedStream(s) holds equivalent extraJoinCondList and extraChildCondList and agent.
 * 
 * @JoinDelayedStream
 * @author tjy
 *
 */
public class JoinDelayedStreamContainer extends
		JoinStreamContainer {
	private static final long serialVersionUID = -811026889377484551L;
	JoinStreamContainer agent;
	List<AbstractBooleanExpression> extraJoinCondList=new ArrayList<AbstractBooleanExpression>(2);
	List<AbstractBooleanExpression> extraChildCondList=new ArrayList<AbstractBooleanExpression>(4);
	
	public JoinDelayedStreamContainer(){
	}
	public JoinDelayedStreamContainer(
			JoinDelayedStream jcsl, JoinStreamContainer agent){
		//this.agent=agent;
		this.setAgent(agent);
		for(AbstractBooleanExpression extraJoinCond: jcsl.getExtraJoinCondList()){
			extraJoinCondList.add((AbstractBooleanExpression)cloner.deepClone(extraJoinCond));
		}
		for(AbstractBooleanExpression extraChildCond: jcsl.getExtraChildCondList()){
			extraChildCondList.add((AbstractBooleanExpression)cloner.deepClone(extraChildCond));
		}
		//copy Result Element List
		for(SelectClauseExpressionElement agentSe: agent.getResultElementList()){
			this.addResultElement(new SelectExpressionElementContainer(agentSe));
		}
		this.setWindowTimeUS(jcsl.getWindowTimeUS());
		this.setWorkerId(jcsl.getWorkerId());
	}
	
	public JoinStreamContainer getAgent() {
		return agent;
	}
	public void setAgent(JoinStreamContainer agent) {
		this.agent = agent;
		if(agent!=null){
			this.agent.addDownContainerId(this.id);
		}
	}
	public List<AbstractBooleanExpression> getExtraJoinCondList() {
		return extraJoinCondList;
	}
	public List<AbstractBooleanExpression> getExtraChildCondList() {
		return extraChildCondList;
	}
	public List<AbstractBooleanExpression> getJoinExprList() {
		List<AbstractBooleanExpression> exprList=new ArrayList<AbstractBooleanExpression>();
		exprList.addAll(agent.getJoinExprList());
		exprList.addAll(extraJoinCondList);
		exprList.addAll(extraChildCondList);
		return exprList;
	}
	public StreamContainer getUpContainer(int index){
		return agent.getUpContainer(index);
	}

	public void setJoinExprList(List<AbstractBooleanExpression> joinExprList) {
		agent.setJoinExprList(joinExprList);
	}

	public ArrayList<StreamContainer> getUpContainerList() {
		return agent.getUpContainerList();
	}
	
	public int getUpContainerCount(){
		return agent.getUpContainerCount();
	}
	
	public void setExtraJoinCondList(
			List<AbstractBooleanExpression> extraJoinCondList) {
		this.extraJoinCondList = extraJoinCondList;
	}
	public void setExtraChildCondList(
			List<AbstractBooleanExpression> extraChildCondList) {
		this.extraChildCondList = extraChildCondList;
	}
	public void addUpContainerIfNotExists(StreamContainer child){
		agent.addUpContainerIfNotExists(child);
	}
	public void addUpContainersIfNotExist(List<StreamContainer> childList){
		agent.addUpContainersIfNotExist(childList);
	}

	@Override
	public Stream[] getUpStreams(){
		return new Stream[]{agent};
	}
	
	@Override
	public Stream getUpStreamByEventName(
			String childEventName) {
		return agent;
	}
	
	@Override
	public int getLevel() {
		return agent.getLevel()+1;
	}
	
	@Override
	protected void mergeJoinExprList(DeepReplaceFactory replacer){
		for(AbstractBooleanExpression extraJoinCond: extraJoinCondList){
			replacer.deepReplace(extraJoinCond);
		}
		for(AbstractBooleanExpression extraChildCond: extraChildCondList){
			replacer.deepReplace(extraChildCond);
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
		for(AbstractBooleanExpression extraJoinCond: this.extraJoinCondList){
			extraJoinCond.dumpConjunctionExpressions(ownCondList);
		}
		for(AbstractBooleanExpression extraChildCond: this.extraChildCondList){
			extraChildCond.dumpConjunctionExpressions(ownCondList);
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
		if(this.extraChildCondList.size()+this.extraJoinCondList.size()<=0){
			return null;
		}
		else if(this.extraChildCondList.size()+this.extraJoinCondList.size()<=1){
			if(this.extraChildCondList.size()==1){
				return this.extraChildCondList.get(0);
			}
			else{
				return this.extraJoinCondList.get(0);
			}
		}
		CompositeExpression ce=new CompositeExpression(RelationTypeEnum.AND);
		for(AbstractBooleanExpression cc: this.extraChildCondList){
			ce.addChildExpr(cc);
		}
		for(AbstractBooleanExpression jc: this.extraJoinCondList){
			ce.addChildExpr(jc);
		}
		return ce;
	}
}
