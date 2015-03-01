package dist.esper.epl.expr.util;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;

public interface IExpressionVisitor2<T, E> {
	public T visitComparisionExpression(ComparisonExpression ce, E obj);
	public T visitCompositeExpression(CompositeExpression ce, E obj);
	public T visitEventSpecification(EventSpecification es, E obj);
	public T visitEventIndexedSpecification(EventIndexedSpecification eis, E obj);
	public T visitEventPropertySpecification(EventPropertySpecification eps, E obj);
	public T visitEventPropertyIndexedSpecification(EventPropertyIndexedSpecification epis, E obj);
	public T visitTimePeriod(TimePeriod t, E obj);
	public T visitValue(Value v, E obj);
	public T visitWildcardExpression(WildcardExpression we, E obj);
	public T visitAggregationExpression(AggregationExpression ae, E obj);
	public T visitMathExpression(MathExpression me, E obj);
	public T visitUDFDotExpressionItem(UDFDotExpressionItem dei, E obj);
	public T visitUDFDotExpression(UDFDotExpression de, E obj);
	public T visitPattenMultiChildNode(PatternMultiChildNode pmcn, E obj);
	public T visitPattenFilterNode(PatternFilterNode pncn, E obj);
	public T visitPattenSingleChildNode(PatternSingleChildNode pscn, E obj);
}
