package dist.esper.epl.expr.util;
import java.util.*;

import dist.esper.epl.expr.*;

public class EventAliasDumper extends AbstractExpressionVisitor2<Object, Set<EventAlias>> {

	private static EventAliasDumper instance=new EventAliasDumper();
	public static Set<EventAlias> dump(AbstractExpression expr){
		return dump(expr, null);
	}
	public static Set<EventAlias> dump(AbstractExpression expr, Set<EventAlias> eaSet){
		if(eaSet==null){
			eaSet=new HashSet<EventAlias>();
		}
		expr.accept(instance, eaSet);
		return eaSet;
	}
	
	public static <E extends AbstractExpression> Set<EventAlias> dump(
			List<E> exprList , Set<EventAlias> eaSet){
		if(eaSet==null){
			eaSet=new HashSet<EventAlias>();
		}
		for(E expr: exprList){
			dump(expr, eaSet);
		}
		return eaSet;
	}
	@Override
	public Object visitEventSpecification(EventSpecification es,
			Set<EventAlias> obj) {
		obj.add(es.getEventAlias());
		return null;
	}

	@Override
	public Object visitEventPropertySpecification(
			EventPropertySpecification eps, Set<EventAlias> obj) {
		eps.getEventSpec().accept(this, obj);
		return null;
	}
}
