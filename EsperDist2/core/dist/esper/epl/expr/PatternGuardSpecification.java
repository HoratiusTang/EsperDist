package dist.esper.epl.expr;


import java.util.*;

import com.espertech.esper.epl.expression.*;
import com.espertech.esper.epl.spec.PatternGuardSpec;

public class PatternGuardSpecification extends ObjectSpecification{
	private static final long serialVersionUID = -3940842417787421741L;

	public PatternGuardSpecification(){
		super();
	}
	public PatternGuardSpecification(String namespace, String function) {
		super(namespace,function);
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw){
		if(getNamespace().equals(PatternGuardEnum.WHILE_GUARD.getNamespace()) &&
				getFunction().equals(PatternGuardEnum.WHILE_GUARD.getFunction())){
			sw.append("while ");
		}
		else{
			sw.append("where ");
            sw.append(getNamespace());
            sw.append(":");
            sw.append(getFunction());            
		}
		sw.append("(");
        ExpressionFactory.toEPLParameterList(getParamList(), sw);
        sw.append(")");
	}
	
	public static class Factory{
		public static PatternGuardSpecification make(PatternGuardSpec guardSpec){
			PatternGuardSpecification pg=new PatternGuardSpecification(guardSpec.getObjectNamespace(),guardSpec.getObjectName());
			List<ExprNode> exprList=guardSpec.getObjectParameters();
			for(ExprNode expr: exprList){
				assert((expr instanceof ExprConstantNode) || (expr instanceof ExprIdentNode));
				AbstractResultExpression param=(AbstractResultExpression)ExpressionFactory.toExpression(expr);
				//System.out.println(param.toString());
				pg.addParameter(param);
			}
			return pg;
		}
	}
}
