package dist.esper.epl.expr.pattern;


import java.util.ArrayList;
import java.util.Set;

import com.espertech.esper.epl.expression.ExprNodeUtility;
import com.espertech.esper.epl.spec.PatternGuardSpec;
import com.espertech.esper.epl.spec.PatternObserverSpec;
import com.espertech.esper.pattern.EvalGuardFactoryNode;
import com.espertech.esper.pattern.EvalNotFactoryNode;
import com.espertech.esper.pattern.EvalObserverFactoryNode;
import com.espertech.esper.pattern.PatternExpressionPrecedenceEnum;

import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.ExpressionFactory;
import dist.esper.epl.expr.PatternGuardSpecification;
import dist.esper.epl.expr.PatternObserverSpecification;

public class PatternObserverNode extends PatternSingleChildNode{
	private static final long serialVersionUID = 6624918503464984036L;
	PatternObserverSpecification observerSpec=null;
	
	public PatternObserverNode() {
		super();
	}

	public PatternObserverNode(AbstractPatternNode childNode) {
		super(childNode);
	}
	
	public PatternObserverNode(EvalObserverFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
	}	
	
	public PatternObserverSpecification getObserverSpec() {
		return observerSpec;
	}

	public void setObserverSpec(PatternObserverSpecification observerSpec) {
		this.observerSpec = observerSpec;
	}

	public static class Factory{
		public static PatternObserverNode make(EvalObserverFactoryNode factoryNode, AbstractPatternNode parent){
			PatternObserverNode pon=new PatternObserverNode(factoryNode, parent);
			PatternObserverSpec pgs=((EvalObserverFactoryNode)factoryNode).getPatternObserverSpec();
			pon.setObserverSpec(PatternObserverSpecification.Factory.make(pgs));
			return pon;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		observerSpec.toStringBuilder(sw);
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.ATOM;
	}
}
