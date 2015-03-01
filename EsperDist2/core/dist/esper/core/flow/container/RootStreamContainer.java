package dist.esper.core.flow.container;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.core.flow.stream.DerivedStream;
import dist.esper.core.flow.stream.RootStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.CompositeExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.DeepReplaceFactory;

/**
 * the container contains @RootStream(s),
 * each of the @RootStream(s) holds equivalent whereExprList and upStream.
 * 
 * @see @RootStream
 * @author tjy
 *
 */
public class RootStreamContainer extends DerivedStreamContainer{
	private static final long serialVersionUID = -7285727886405413872L;
	StreamContainer upContainer;
	List<AbstractBooleanExpression> whereExprList=new ArrayList<AbstractBooleanExpression>(2);
	
	public RootStreamContainer(){
	}
	public RootStreamContainer(RootStream rsl){
		for(AbstractBooleanExpression whereExpr: rsl.getWhereExprList()){
			whereExprList.add((AbstractBooleanExpression)cloner.deepClone(whereExpr));
		}
		//this.streamLocationList.add(rsl);
		//this.merge(rsl);
		this.setWindowTimeUS(rsl.getWindowTimeUS());
		this.setWorkerId(rsl.getWorkerId());
	}
	
	public StreamContainer getUpContainer() {
		return upContainer;
	}

	public void setUpContainer(StreamContainer child) {
		this.upContainer = child;
		if(child!=null){
			this.upContainer.addDownContainerId(this.id);
		}
	}

	public List<AbstractBooleanExpression> getWhereExprList() {
		return whereExprList;
	}

	public void setWhereExprList(List<AbstractBooleanExpression> whereExprList) {
		this.whereExprList = whereExprList;
	}

	@Override
	public Stream getUpStreamByEventName(
			String childEventName) {
		if(upContainer.getInternalCompositeEvent().getName().equals(childEventName)){
			return upContainer;
		}
		return null;
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		ContainerStringlizer.toStringBuilder(this, sw, indent);
	}

	@Override
	public void merge(DerivedStream pcsl,
			Map<EventAlias, EventAlias> rscToRslMap,
			BooleanExpressionComparisonResult cr) {
		super.merge(pcsl, rscToRslMap, cr);
		DeepReplaceFactory replacer=new DeepReplaceFactory(rscToRslMap);
		mergeWhereList(replacer);
	}
	
	protected void mergeWhereList(DeepReplaceFactory replacer){
		for(AbstractBooleanExpression whereExpr: whereExprList){
			replacer.deepReplace(whereExpr);
		}
	}	
	
	@Override
	public int getBaseStreamCount() {
		return upContainer.getBaseStreamCount();
	}
	@Override
	public int getRawStreamCount() {
		return upContainer.getRawStreamCount();
	}
	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		((DerivedStreamContainer)upContainer).dumpAllBooleanExpressions(childrenCondList);
	}

	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> ownCondList) {
		ownCondList.addAll(whereExprList);
	}
	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		((DerivedStreamContainer)upContainer).dumpEventAlias(eaSet);
	}
	@Override
	public ViewSpecification[] getViewSpecs() {
		return ((DerivedStreamContainer)upContainer).getViewSpecs();
	}
	@Override
	public int getLevel() {
		return upContainer.getLevel()+1;
	}
	@Override
	public Stream[] getUpStreams() {
		return new Stream[]{upContainer};
	}
	@Override
	public AbstractBooleanExpression getOwnCondition() {
		if(this.whereExprList==null || this.whereExprList.size()<1){
			return null;
		}
		else if(this.whereExprList.size()==1){
			return this.whereExprList.get(0);
		}
		return new CompositeExpression(RelationTypeEnum.AND, this.whereExprList);
	}
}
