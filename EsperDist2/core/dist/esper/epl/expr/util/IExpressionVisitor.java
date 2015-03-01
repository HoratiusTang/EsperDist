package dist.esper.epl.expr.util;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;

public interface IExpressionVisitor<T> {
	public T visitComparisionExpression(ComparisonExpression ce);
	public T visitCompositeExpression(CompositeExpression ce);
	public T visitEventSpecification(EventSpecification es);
	public T visitEventIndexedSpecification(EventIndexedSpecification eis);
	public T visitEventPropertySpecification(EventPropertySpecification eps);
	public T visitEventPropertyIndexedSpecification(EventPropertyIndexedSpecification epis);
	public T visitTimePeriod(TimePeriod t);
	public T visitValue(Value v);
	public T visitWildcardExpression(WildcardExpression we);
	public T visitAggregationExpression(AggregationExpression ae);
	public T visitMathExpression(MathExpression me);
	public T visitUDFDotExpressionItem(UDFDotExpressionItem dei);
	public T visitUDFDotExpression(UDFDotExpression de);
	public T visitPattenMultiChildNode(PatternMultiChildNode pmcn);
	public T visitPattenFilterNode(PatternFilterNode pncn);
	public T visitPattenSingleChildNode(PatternSingleChildNode pscn);
}
