package dist.esper.core.worker.pubsub;

import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.util.ExpressionStringlizer;

/**
 * the special stringlizer only for filter expressions in 'FROM' clause,
 * e.g. 'B(id>6) as b1'.
 * if use ExpressionStringlizer, it will be written as 'B(b1.id>6) as b1',
 * where 'b1.id' can not be recognized by Esper parser.
 * 
 * @author tjy
 *
 */
public class FilterExpressionStringlizer extends ExpressionStringlizer {
	protected static FilterExpressionStringlizer instance=new FilterExpressionStringlizer();
	public static FilterExpressionStringlizer getInstance(){
		return instance;
	}
	
	@Override
	public StringBuilder visitEventPropertySpecification(
			EventPropertySpecification eps, StringBuilder sw) {
		//eps.getEventSpec().accept(this, sw);
		//sw.append(".");
		sw.append(eps.getEventProp().getName());
		return null;
	}
}
