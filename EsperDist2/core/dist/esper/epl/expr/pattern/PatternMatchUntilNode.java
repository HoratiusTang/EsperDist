package dist.esper.epl.expr.pattern;


import java.util.Set;

import com.espertech.esper.epl.expression.*;
import com.espertech.esper.pattern.*;

import dist.esper.epl.expr.*;

public class PatternMatchUntilNode extends PatternSingleChildNode{
	private static final long serialVersionUID = -6731463641609659628L;
	public Value singleBound=null;
	public Value lowerBounds=null;
	public Value upperBounds=null;
	
	public PatternMatchUntilNode() {
		super();
	}

	public PatternMatchUntilNode(AbstractPatternNode childNode) {
		super(childNode);
	}
	
	public PatternMatchUntilNode(EvalMatchUntilFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
	}
	
	public Value getSingleBound() {
		return singleBound;
	}

	public void setSingleBound(Value singleBound) {
		this.singleBound = singleBound;
	}

	public Value getLowerBounds() {
		return lowerBounds;
	}

	public void setLowerBounds(Value lowerBounds) {
		this.lowerBounds = lowerBounds;
	}

	public Value getUpperBounds() {
		return upperBounds;
	}

	public void setUpperBounds(Value upperBounds) {
		this.upperBounds = upperBounds;
	}

	public static class Factory{
		public static PatternMatchUntilNode make(EvalMatchUntilFactoryNode fn, AbstractPatternNode parent){
			PatternMatchUntilNode pmun=new PatternMatchUntilNode(fn, parent);
			ExprNode sb=fn.getSingleBound();
			ExprNode lb=fn.getLowerBounds();
			ExprNode ub=fn.getUpperBounds();
			if(sb!=null){
				assert(sb instanceof ExprConstantNode);
				pmun.setSingleBound((Value)(ExpressionFactory.toExpression(sb)));
			}
			if(lb!=null){
				assert(sb instanceof ExprConstantNode);
				pmun.setLowerBounds((Value)(ExpressionFactory.toExpression(lb)));
			}
			if(ub!=null){
				assert(sb instanceof ExprConstantNode);
				pmun.setUpperBounds((Value)(ExpressionFactory.toExpression(ub)));
			}
			return pmun;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		if(singleBound!=null){
			sw.append("[");
			singleBound.toStringBuilder(sw);
			sw.append("]");
		}
		else if (lowerBounds != null || upperBounds != null){
			sw.append("[");
			if(lowerBounds!=null){
				lowerBounds.toStringBuilder(sw);
			}
			sw.append(":");
			if(upperBounds!=null){
				upperBounds.toStringBuilder(sw);
			}
			sw.append("]");
		}
		childNode.toStringBuilder(sw);
		//FIXME
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.REPEAT_UNTIL;
	}
}
