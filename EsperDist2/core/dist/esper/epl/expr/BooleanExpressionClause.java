package dist.esper.epl.expr;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.espertech.esper.client.soda.Expression;
import com.espertech.esper.epl.expression.ExprNode;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class BooleanExpressionClause extends AbstractClause {
	public AbstractBooleanExpression expr=null;
	
	private List<AbstractBooleanExpression> conjunctList=null;
	
	public BooleanExpressionClause(AbstractBooleanExpression expr, String type){
		super();
		this.expr=expr;
		this.type=type;
	}
	
	public AbstractBooleanExpression getExpr() {
		return expr;
	}

	public void setExpr(AbstractBooleanExpression expr) {
		this.expr = expr;
	}

	public static class Factory{
		public static BooleanExpressionClause make(ExprNode exprNode, String type){
			AbstractBooleanExpression expr=(AbstractBooleanExpression)ExpressionFactory.toExpression(exprNode);
			BooleanExpressionClause clause=new BooleanExpressionClause(expr, type);
			return clause;
		}
		
		public static BooleanExpressionClause make(Expression expr0, String type){
			AbstractBooleanExpression expr=(AbstractBooleanExpression)ExpressionFactory1.toExpression1(expr0);
			BooleanExpressionClause clause=new BooleanExpressionClause(expr, type);
			return clause;
		}
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(type);
		sw.append(" ");
		expr.toStringBuilder(sw);
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param)
			throws Exception {
		super.resolve(ssw, param);
		return expr.resolve(ssw, param);
	}
	
	public List<AbstractBooleanExpression> getConjunctionList(){
		if(this.conjunctList==null){
			this.conjunctList=new ArrayList<AbstractBooleanExpression>(4);
			this.expr.dumpConjunctionExpressions(this.conjunctList);
		}
		return this.conjunctList;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitBooleanExpressionClause(this);
	}
}
