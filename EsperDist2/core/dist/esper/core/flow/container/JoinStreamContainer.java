package dist.esper.core.flow.container;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.core.flow.stream.DerivedStream;
import dist.esper.core.flow.stream.JoinStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.CompositeExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.TimePeriod;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.DeepReplaceFactory;
import dist.esper.event.Event;

/**
 * the container contains @JoinStream(s),
 * each of the @JoinStream(s) holds equivalent joinExprList and up-streams.
 * 
 * @see @JoinStream
 * @author tjy
 *
 */
public class JoinStreamContainer extends DerivedStreamContainer{
	private static final long serialVersionUID = 782243892278615106L;
	ArrayList<StreamContainer> upContainerList=new ArrayList<StreamContainer>(2);
	List<AbstractBooleanExpression> joinExprList=new ArrayList<AbstractBooleanExpression>(2);
	
	public JoinStreamContainer(){
	}
	
	public JoinStreamContainer(JoinStream jsl){
		for(AbstractBooleanExpression joinExpr: jsl.getJoinExprList()){
			joinExprList.add((AbstractBooleanExpression)cloner.deepClone(joinExpr));
		}
		this.setWindowTimeUS(jsl.getWindowTimeUS());
		this.setWorkerId(jsl.getWorkerId());
	}

	public List<AbstractBooleanExpression> getJoinExprList() {
		return joinExprList;
	}

	public void setJoinExprList(List<AbstractBooleanExpression> joinExprList) {
		this.joinExprList = joinExprList;
	}

	public ArrayList<StreamContainer> getUpContainerList() {
		return upContainerList;
	}
	
	public void setUpContainerList(ArrayList<StreamContainer> childList) {
		this.upContainerList = childList;
//		for(StreamLocationContainer child: this.childList){
//			child.addDownContainerId(this.id);
//		}
	}

	public StreamContainer getUpContainer(int index){
		return upContainerList.get(index);
	}
	
	public int getUpContainerCount(){
		return upContainerList.size();
	}
	
	public void addUpContainerIfNotExists(StreamContainer child){
		if(child!=null && !upContainerList.contains(child)){
			upContainerList.add(child);
			child.addDownContainerId(this.id);
		}
	}
	public void addUpContainersIfNotExist(List<StreamContainer> childList){
		for(StreamContainer child: childList){
			addUpContainerIfNotExists(child);
		}
	}

	@Override
	public Stream getUpStreamByEventName(
			String childEventName) {
		for(StreamContainer child: upContainerList){
			if(child.getInternalCompositeEvent().getName().equals(childEventName)){
				return child;
			}
		}
		return null;
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		ContainerStringlizer.toStringBuilder(this, sw, indent);
	}
	
	@Override
	public void merge(DerivedStream pcsl,
			Map<EventAlias, EventAlias> jscToJslMap,
			BooleanExpressionComparisonResult cr) {
		super.merge(pcsl, jscToJslMap, cr);
		DeepReplaceFactory replacer=new DeepReplaceFactory(jscToJslMap);
		mergeJoinExprList(replacer);
	}
	
	protected void mergeJoinExprList(DeepReplaceFactory replacer){
		for(AbstractBooleanExpression joinExpr: joinExprList){
			replacer.deepReplace(joinExpr);
		}
	}

	@Override
	public int getBaseStreamCount() {
		int count=0;
		for(StreamContainer child: this.getUpContainerList()){
			count+=child.getBaseStreamCount();
		}
		return count;
	}

	@Override
	public int getRawStreamCount() {
		int count=0;
		for(StreamContainer child: this.getUpContainerList()){
			count+=child.getRawStreamCount();
		}
		return count;
	}
	
	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		for(StreamContainer child: upContainerList){
			((DerivedStreamContainer)child).dumpAllBooleanExpressions(childrenCondList);
		}
	}
	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> ownCondList) {
		ownCondList.addAll(joinExprList);
	}
	
	@Override
	public void dumpEvents(Collection<Event> events) {
		for(Stream child: upContainerList){
			((DerivedStreamContainer)child).dumpEvents(events);
		}
	}

	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		for(StreamContainer child: upContainerList){
			((DerivedStreamContainer)child).dumpEventAlias(eaSet);
		}
	}

	@Override
	public ViewSpecification[] getViewSpecs() {
		TimePeriod maxTimePeriod=null;
		//FIXME
		for(Stream child: upContainerList){
			ViewSpecification[] vss=((DerivedStream)child).getViewSpecs();
			if(vss!=null){
				TimePeriod t=(TimePeriod)vss[0].getParamList().get(0);
				if(maxTimePeriod==null || TimePeriod.compare(t, maxTimePeriod)>0){
					maxTimePeriod=t;
				}
			}
		}
		return new ViewSpecification[]{new ViewSpecification("win","time", maxTimePeriod)};
	}

	@Override
	public int getLevel() {
		int childMaxLevel=0;
		for(StreamContainer child: upContainerList){
			childMaxLevel=child.getLevel()>childMaxLevel?child.getLevel():childMaxLevel;
		}
		return childMaxLevel+1;
	}

	@Override
	public Stream[] getUpStreams() {
		Stream[] upStreams=new Stream[this.upContainerList.size()];
		int i=0;
		for(StreamContainer upContainer: this.upContainerList){
			upStreams[i]=upContainer;
			i++;
		}
		return upStreams;
	}

	@Override
	public AbstractBooleanExpression getOwnCondition() {
		if(this.joinExprList==null || this.joinExprList.size()<1){
			return null;
		}
		else if(this.joinExprList.size()==1){
			return this.joinExprList.get(0);
		}
		return new CompositeExpression(RelationTypeEnum.AND, this.joinExprList);
	}
	
	@Override
	public void dumpAllUpStreamContainers(List<StreamContainer> dscList) {
		for(StreamContainer upContainer: this.upContainerList){
			upContainer.dumpAllUpStreamContainers(dscList);
		}
		dscList.add(this);
	}
}
