package dist.esper.epl.expr;



import dist.esper.epl.expr.util.IClauseVisitor;


public class SelectClauseWildcardElement extends SelectClauseElement{
	private static final long serialVersionUID = 1488672572800269123L;
	private static final SelectClauseWildcardElement instance=new SelectClauseWildcardElement();
	public static SelectClauseWildcardElement getInstance(){
		return instance;
	}	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append("*");
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitSelectClauseWildcardElement(this);
	}
}
