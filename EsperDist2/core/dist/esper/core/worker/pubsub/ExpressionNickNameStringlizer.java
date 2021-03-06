package dist.esper.core.worker.pubsub;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.ExpressionStringlizer;

class ExpressionNickNameStringlizer extends ExpressionStringlizer{
	protected static ExpressionNickNameStringlizer instance2=new ExpressionNickNameStringlizer();
	
	public static ExpressionNickNameStringlizer getInstance(){
		return instance2;
	}
	
	public void toStringBuilder(SelectClauseExpressionElement se, StringBuilder sw){
		se.getSelectExpr().accept(this,sw);
	}
	
	public StringBuilder visitEventOrPropertySpecification(
			EventOrPropertySpecification eops, StringBuilder sw){
		sw.append(eops.getInternalEventNickName()+"."+eops.getInternalEventPropertyName());
		return sw;
	}
	
	@Override
	public StringBuilder visitEventSpecification(EventSpecification es,
			StringBuilder sw) {
		return visitEventOrPropertySpecification(es, sw);
	}

	@Override
	public StringBuilder visitEventIndexedSpecification(EventIndexedSpecification eis,
			StringBuilder sw) {
		return visitEventOrPropertySpecification(eis, sw);
	}

	@Override
	public StringBuilder visitEventPropertySpecification(
			EventPropertySpecification eps, StringBuilder sw) {
		return visitEventOrPropertySpecification(eps, sw);
	}

	@Override
	public StringBuilder visitEventPropertyIndexedSpecification(
			EventPropertyIndexedSpecification epis, StringBuilder sw) {
		return visitEventOrPropertySpecification(epis, sw);
	}
	
}
