package dist.esper.epl.expr.pattern;


import java.util.Set;

import com.espertech.esper.pattern.EvalEveryDistinctFactoryNode;
import com.espertech.esper.pattern.EvalEveryFactoryNode;
import com.espertech.esper.pattern.PatternExpressionPrecedenceEnum;

import dist.esper.epl.expr.EventAlias;

public class PatternEveryNode extends PatternSingleChildNode{
	private static final long serialVersionUID = 1472416922249949238L;

	public PatternEveryNode() {
		super();
	}

	public PatternEveryNode(AbstractPatternNode childNode) {
		super(childNode);
	}

	public PatternEveryNode(EvalEveryFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append("(every ");
		this.childNode.toStringBuilder(sw);
		sw.append(")");
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.EVERY;
	}
}
