package dist.esper.epl.expr.pattern;


import java.util.ArrayList;
import java.util.Set;

import com.espertech.esper.epl.expression.ExprNodeUtility;
import com.espertech.esper.epl.spec.PatternGuardSpec;
import com.espertech.esper.pattern.EvalFollowedByFactoryNode;
import com.espertech.esper.pattern.EvalGuardFactoryNode;
import com.espertech.esper.pattern.PatternExpressionPrecedenceEnum;

import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.ExpressionFactory;
import dist.esper.epl.expr.PatternGuardEnum;
import dist.esper.epl.expr.PatternGuardSpecification;

public class PatternGuardNode extends PatternSingleChildNode{
	private static final long serialVersionUID = -2837456950009729035L;
	public PatternGuardSpecification guardSpec=null;
	
	public PatternGuardNode() {
		super();
	}

	public PatternGuardNode(AbstractPatternNode childNode) {
		super(childNode);
	}
	
	public PatternGuardNode(EvalGuardFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
	}
	
	public PatternGuardSpecification getGuardSpec() {
		return guardSpec;
	}

	public void setGuardSpec(PatternGuardSpecification guardSpec) {
		this.guardSpec = guardSpec;
	}
	
	public static class Factory{
		public static PatternGuardNode make(EvalGuardFactoryNode factoryNode, AbstractPatternNode parent){
			PatternGuardNode pgn=new PatternGuardNode(factoryNode, parent);
			PatternGuardSpec pgs=((EvalGuardFactoryNode)factoryNode).getPatternGuardSpec();
			pgn.setGuardSpec(PatternGuardSpecification.Factory.make(pgs));
			return pgn;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append("(");
		childNode.toStringBuilder(sw);
		sw.append(" ");
		guardSpec.toStringBuilder(sw);
		sw.append(")");
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.GUARD_POSTFIX;
	}
}
