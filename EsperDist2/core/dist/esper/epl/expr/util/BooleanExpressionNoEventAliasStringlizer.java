package dist.esper.epl.expr.util;

import dist.esper.epl.expr.EventSpecification;

/**
 * usage: stringlize "b.id>2" to "B.id>2", where B is the event name.
 * @author tjy
 *
 */
public class BooleanExpressionNoEventAliasStringlizer extends
		ExpressionStringlizer {
	protected static BooleanExpressionNoEventAliasStringlizer instance=new BooleanExpressionNoEventAliasStringlizer();
	
	public static BooleanExpressionNoEventAliasStringlizer getInstance(){
		return instance;
	}
	
	@Override
	public StringBuilder visitEventSpecification(EventSpecification es,
			StringBuilder sw) {
		sw.append(es.getEventAlias().getEvent().getName());
		return sw;
	}
}
