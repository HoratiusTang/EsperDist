package dist.esper.epl.expr.pattern;


import java.util.ArrayList;
import java.util.Set;

import com.espertech.esper.pattern.EvalAndFactoryNode;
import com.espertech.esper.pattern.PatternExpressionPrecedenceEnum;

import dist.esper.epl.expr.EventAlias;

public class PatternAndNode extends PatternMultiChildNode{	
	
	public PatternAndNode() {
		super();
	}

	public PatternAndNode(EvalAndFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		PatternNodeFactory.toEPL(sw, childNodeList, this.getPrecedence());
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.AND;
	}
}
