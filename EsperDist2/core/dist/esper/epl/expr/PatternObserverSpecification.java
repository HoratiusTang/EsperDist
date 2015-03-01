package dist.esper.epl.expr;


import java.util.List;

import com.espertech.esper.epl.expression.*;
import com.espertech.esper.epl.spec.PatternObserverSpec;

public class PatternObserverSpecification extends ObjectSpecification{
	private static final long serialVersionUID = -8459612979411153929L;

	public PatternObserverSpecification(){
		super();
	}
	public PatternObserverSpecification(String namespace, String function) {
		super(namespace,function);
	}
	
	public static class Factory{
		public static PatternObserverSpecification make(PatternObserverSpec guardSpec){
			PatternObserverSpecification pg=new PatternObserverSpecification(guardSpec.getObjectNamespace(),guardSpec.getObjectName());
			List<ExprNode> exprList=guardSpec.getObjectParameters();
			for(ExprNode expr: exprList){
				assert((expr instanceof ExprConstantNode) || (expr instanceof ExprIdentNode));
				AbstractIdentExpression para=(AbstractIdentExpression)(ExpressionFactory.toExpression(expr));
				pg.addParameter(para);
			}
			return pg;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(getNamespace());
        sw.append(":");
        sw.append(getFunction());
        sw.append("(");
        ExpressionFactory.toEPLParameterList(getParamList(), sw);
        sw.append(")");		
	}
}
