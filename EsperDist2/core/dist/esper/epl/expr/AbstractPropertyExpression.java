package dist.esper.epl.expr;


import java.util.Set;

public abstract class AbstractPropertyExpression extends AbstractExpression {	
	private static final long serialVersionUID = 910881619084611203L;
	/*public abstract void toStringBuilderWithNickName(StringBuilder sw);*/
	public boolean resolveReference(){
		return true;
	}
}
