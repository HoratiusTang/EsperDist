package dist.esper.core.flow.stream;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.pattern.AbstractPatternNode;
import dist.esper.util.StringUtil;

/**
 * the stream presents a pattern stream in 'FROM' clause,
 * e.g. 'pattern [every [2](a=A-> [2]b=B(b[0].age>a.age))].win:time(5 second)'
 * it might have multiple up-streams, which are all @RawStream(s).
 * 
 * @see @PatternNode
 * @author tjy
 *
 */
public class PatternStream extends BaseStream{
	private static final long serialVersionUID = -1225701291244715016L;
	public AbstractPatternNode patternNode=null;
	List<RawStream> rawStreamList=new ArrayList<RawStream>(2);

	public PatternStream() {
		super();		
	}
	
	public PatternStream(AbstractPatternNode patternNode) {
		super();
		this.patternNode = patternNode;
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
		this.rawStreamList.addAll(rawStreamList);
	}
	
	public void addRawStream(RawStream rsw) {
		this.rawStreamList.add(rsw);
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		StreamStringlizer.toStringBuilder(this, sw, indent);
	}

//	@Override
//	public Stream getChildStreamLocationByEventName(
//			String childEventName) {
//		for(Stream child: rawStreamList){
//			if(child.getInternalCompositeEvent().getName().equals(childEventName)){
//				return child;
//			}
//		}
//		return null;
//	}

	@Override
	public int getBaseStreamCount() {
		return 1;
	}

	@Override
	public int getRawStreamCount() {
		return this.rawStreamList.size();
	}

	@Override
	public boolean tryAddResultElement(EventOrPropertySpecification eps) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> curCondList) {
		throw new RuntimeException("not implemented yet");
	}
}
