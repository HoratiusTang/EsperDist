package dist.esper.epl.expr.util;
import java.util.*;

import dist.esper.epl.expr.*;

public class EventOrPropertySpecReferenceDumper extends AbstractExpressionVisitor2<Object, Set<EventOrPropertySpecification>> {

	private static EventOrPropertySpecReferenceDumper instance=new EventOrPropertySpecReferenceDumper();
	public static Set<EventOrPropertySpecification> dump(AbstractExpression expr){
		return dump(expr, null);
	}
	public static Set<EventOrPropertySpecification> dump(AbstractExpression expr, Set<EventOrPropertySpecification> epsSet){
		if(epsSet==null){
			epsSet=new HashSet<EventOrPropertySpecification>();
		}
		expr.accept(instance, epsSet);
		return epsSet;
	}
	public static <E extends AbstractExpression> Set<EventOrPropertySpecification> dump(
			List<E> exprList, Set<EventOrPropertySpecification> epsSet){
		if(epsSet==null){
			epsSet=new HashSet<EventOrPropertySpecification>();
		}
		for(E expr: exprList){
			dump(expr, epsSet);
		}
		return epsSet;
	}
	@Override
	public Object visitEventSpecification(EventSpecification es,
			Set<EventOrPropertySpecification> obj) {
		obj.add(es);
		return null;
	}
	
	@Override
	public Object visitEventPropertySpecification(
			EventPropertySpecification eps, Set<EventOrPropertySpecification> obj) {
		obj.add(eps);
		return null;
	}
}
