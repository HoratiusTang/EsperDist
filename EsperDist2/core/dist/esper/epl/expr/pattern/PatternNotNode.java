package dist.esper.epl.expr.pattern;


import java.util.Set;

import com.espertech.esper.pattern.EvalEveryFactoryNode;
import com.espertech.esper.pattern.EvalNotFactoryNode;
import com.espertech.esper.pattern.PatternExpressionPrecedenceEnum;

import dist.esper.epl.expr.EventAlias;

public class PatternNotNode extends PatternSingleChildNode{
	private static final long serialVersionUID = 3827101515572104685L;

	public PatternNotNode() {
		super();
	}

	public PatternNotNode(AbstractPatternNode childNode) {
		super(childNode);
	}
	
	public PatternNotNode(EvalNotFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append("(not ");
		childNode.toStringBuilder(sw);
		sw.append(")");
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.NOT;
	}
}
