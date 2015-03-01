package dist.esper.epl.expr;


import java.util.Set;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class WildcardExpression extends AbstractIdentExpression {
	private static final long serialVersionUID = -855909345554911944L;
	private static final WildcardExpression instance=new WildcardExpression();
	private static String string="*";
	
	public WildcardExpression(){}
	
	public static WildcardExpression getInstance(){
		return instance;
	}
	
	@Override
	public int hashCode(){
		return string.hashCode();
	}
	
	@Override
	public String toString(){
		return string;
	}
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(string);
	}
	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) {
		// TODO Auto-generated method stub
		return true;
	}
	
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitWildcardExpression(this);
	}
	
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitWildcardExpression(this, obj);
	}
}
