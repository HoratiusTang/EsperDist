package dist.esper.epl.expr.util;

import java.util.*;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;

public abstract class AbstractExpressionVisitor2<T, E> implements IExpressionVisitor2<T, E> {

	@Override
	public T visitComparisionExpression(ComparisonExpression ce, E obj) {
		for(AbstractResultExpression child: ce.getChildExprList()){
			child.accept(this, obj);
		}
		return null;
	}

	@Override
	public T visitCompositeExpression(CompositeExpression ce, E obj) {
		for(AbstractBooleanExpression child: ce.getChildExprList()){
			child.accept(this, obj);
		}
		return null;
	}
	
	@Override
	public T visitEventIndexedSpecification(EventIndexedSpecification eis, E obj) {
		return this.visitEventSpecification(eis, obj);
	}
	
	@Override
	public T visitEventPropertyIndexedSpecification(EventPropertyIndexedSpecification epis, E obj) {
		return this.visitEventPropertySpecification(epis, obj);
	}

	@Override
	public T visitTimePeriod(TimePeriod t, E obj) {
		return null;
	}

	@Override
	public T visitValue(Value v, E obj) {
		return null;
	}

	@Override
	public T visitWildcardExpression(WildcardExpression we, E obj) {
		return null;
	}

	@Override
	public T visitAggregationExpression(AggregationExpression ae, E obj) {
		ae.getExpr().accept(this, obj);
		if(ae.getFilterExpr()!=null){
			ae.getFilterExpr().accept(this, obj);
		}
		return null;
	}

	@Override
	public T visitMathExpression(MathExpression me, E obj) {
		for(AbstractPropertyExpression child: me.getChildExprList()){
			child.accept(this, obj);
		}
		return null;
	}

	@Override
	public T visitUDFDotExpressionItem(UDFDotExpressionItem dei, E obj) {
		for(AbstractPropertyExpression param: dei.getParamList()){
			param.accept(this, obj);
		}
		return null;
	}

	@Override
	public T visitUDFDotExpression(UDFDotExpression de, E obj) {
		for(UDFDotExpressionItem item: de.getItemList()){
			item.accept(this, obj);
		}
		return null;
	}

	@Override
	public T visitPattenMultiChildNode(PatternMultiChildNode pmcn, E obj) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public T visitPattenFilterNode(PatternFilterNode pncn, E obj) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public T visitPattenSingleChildNode(PatternSingleChildNode pscn, E obj) {
		throw new RuntimeException("not implemented yet");
	}
}
