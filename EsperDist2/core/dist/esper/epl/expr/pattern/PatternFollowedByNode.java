package dist.esper.epl.expr.pattern;


import java.util.ArrayList;
import java.util.Set;

import com.espertech.esper.pattern.EvalAndFactoryNode;
import com.espertech.esper.pattern.EvalFollowedByFactoryNode;
import com.espertech.esper.pattern.PatternExpressionPrecedenceEnum;

import dist.esper.epl.expr.EventAlias;

public class PatternFollowedByNode extends PatternMultiChildNode{
		
	public PatternFollowedByNode() {
		super();
		// TODO Auto-generated constructor stub
	}

	public PatternFollowedByNode(EvalFollowedByFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		PatternNodeFactory.toEPL(sw, childNodeList, this.getPrecedence());
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.FOLLOWEDBY;
	}
}
