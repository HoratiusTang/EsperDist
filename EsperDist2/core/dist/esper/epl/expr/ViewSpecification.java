package dist.esper.epl.expr;


import java.util.List;

import com.espertech.esper.client.soda.Expression;
import com.espertech.esper.client.soda.View;
import com.espertech.esper.epl.expression.ExprConstantNode;
import com.espertech.esper.epl.expression.ExprIdentNode;
import com.espertech.esper.epl.expression.ExprNode;
import com.espertech.esper.epl.expression.ExprTimePeriod;
import com.espertech.esper.epl.spec.*;

public class ViewSpecification extends ObjectSpecification {
	private static final long serialVersionUID = -6873261068575372177L;

	public ViewSpecification(){
		super();
	}
	
	public ViewSpecification(String namespace, String function) {
		super(namespace, function);		
	}
	
	public ViewSpecification(String namespace, String function, AbstractResultExpression param) {
		super(namespace, function);
		this.addParameter(param);
	}
	
	public static class Factory{
		public static ViewSpecification make(ViewSpec viewSpec){
			ViewSpecification vs=new ViewSpecification(viewSpec.getObjectNamespace(),viewSpec.getObjectName());
			List<ExprNode> exprList=viewSpec.getObjectParameters();
			for(ExprNode expr: exprList){
				assert((expr instanceof ExprConstantNode) 
						|| (expr instanceof ExprIdentNode)
						|| (expr instanceof ExprTimePeriod)):expr.toString();
				AbstractResultExpression param=(AbstractResultExpression)ExpressionFactory.toExpression(expr);
				vs.addParameter(param);
			}
			return vs;
		}
		public static ViewSpecification[] makeViewSpecs(ViewSpec[] viewSpecs){
			ViewSpecification[] vs=new ViewSpecification[viewSpecs.length];
			for(int i=0;i<vs.length;i++){
				vs[i]=make(viewSpecs[i]);
			}
			return vs;
		}
		
		public static ViewSpecification make(View v0){
			ViewSpecification v1=new ViewSpecification(v0.getNamespace(), v0.getName());
			for(Expression param0: v0.getParameters()){
				AbstractResultExpression param1=(AbstractResultExpression)ExpressionFactory1.toExpression1(param0);
				v1.addParameter(param1);
			}
			return v1;
		}
		
		public static ViewSpecification[] makeViewSpecs(List<View> vs0){
			ViewSpecification[] vs1=new ViewSpecification[vs0.size()];
			for(int i=0;i<vs1.length;i++){
				vs1[i]=make(vs0.get(i));
			}
			return vs1;
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
