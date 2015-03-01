package dist.esper.epl.expr;



import dist.esper.epl.sementic.*;

public class OrderByElement extends AbstractClause{
	AbstractIdentExpression expr=null;
	boolean asc=true;
	
	public AbstractIdentExpression getExpr() {
		return expr;
	}

	public void setExpr(AbstractIdentExpression expr) {
		this.expr = expr;
	}

	public boolean isAsc() {
		return asc;
	}

	public void setAsc(boolean asc) {
		this.asc = asc;
	}

	public OrderByElement(AbstractIdentExpression item, boolean asc) {
		super();
		this.expr = item;
		this.asc = asc;
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		expr.toStringBuilder(sw);
		if(!asc){
			sw.append(" desc");
		}
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		if(!expr.resolve(ssw, null)){
			expr=(AbstractIdentExpression)ExpressionFactory.resolve(expr,ssw,param);
		}
		return true;
	}
}
