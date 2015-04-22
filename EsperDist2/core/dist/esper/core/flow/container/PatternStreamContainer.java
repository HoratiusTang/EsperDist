package dist.esper.core.flow.container;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.core.flow.stream.DerivedStream;
import dist.esper.core.flow.stream.FilterStream;
import dist.esper.core.flow.stream.JoinStream;
import dist.esper.core.flow.stream.PatternStream;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.pattern.AbstractPatternNode;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.util.StringUtil;

/**
 * the container contains @PatternStream(s),
 * each of the @PatternStream(s) holds equivalent pattern and view specification.
 * 
 * @author tjy
 *
 */
public class PatternStreamContainer extends BaseStreamContainer{
	
	private static final long serialVersionUID = -5064640643100439693L;
	AbstractPatternNode patternNode=null;
	List<RawStream> rawStreamList=new ArrayList<RawStream>(2);
	
	public PatternStreamContainer(){
	}
	
	public PatternStreamContainer(PatternStream psl){
		this.patternNode=(AbstractPatternNode)cloner.deepClone(psl.getPatternNode());
		this.rawStreamList.addAll(psl.getRawStreamList());
		//this.patternStreamLocationList.add(psl);
		//this.merge(psl);
	}
	
	public AbstractPatternNode getPatternNode() {
		return patternNode;
	}

	public void setPatternNode(AbstractPatternNode patternNode) {
		this.patternNode = patternNode;
	}

	public List<RawStream> getRawStreamList() {
		return rawStreamList;
	}

	public void setRawStreamList(
			List<RawStream> rawStreamList) {
		this.rawStreamList = rawStreamList;
	}

	@Override
	public Stream getUpStreamByEventName(
			String childEventName) {
		for(Stream child: rawStreamList){
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
	public int getBaseStreamCount() {
		return 1;
	}

	@Override
	public int getRawStreamCount() {
		return this.rawStreamList.size();
	}

	@Override
	public void merge(DerivedStream pcsl,
			Map<EventAlias, EventAlias> scToSlMap,
			BooleanExpressionComparisonResult cr) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> ownCondList) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public Stream[] getUpStreams() {
		Stream[] upLocations=new Stream[this.rawStreamList.size()];		
		int i=0;
		for(Stream child: this.rawStreamList){
			upLocations[i]=child;
			i++;
		}
		return upLocations;		
	}

	@Override
	public AbstractBooleanExpression getOwnCondition() {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void dumpAllUpStreamContainers(List<StreamContainer> dscList) {
		throw new RuntimeException("not implemented yet");
	}
}
