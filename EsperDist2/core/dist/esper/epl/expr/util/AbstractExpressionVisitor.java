package dist.esper.epl.expr.util;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.AbstractPropertyExpression;
import dist.esper.epl.expr.AbstractResultExpression;
import dist.esper.epl.expr.AggregationExpression;
import dist.esper.epl.expr.ComparisonExpression;
import dist.esper.epl.expr.CompositeExpression;
import dist.esper.epl.expr.EventIndexedSpecification;
import dist.esper.epl.expr.EventPropertyIndexedSpecification;
import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.MathExpression;
import dist.esper.epl.expr.TimePeriod;
import dist.esper.epl.expr.UDFDotExpressionItem;
import dist.esper.epl.expr.UDFDotExpression;
import dist.esper.epl.expr.Value;
import dist.esper.epl.expr.WildcardExpression;
import dist.esper.epl.expr.pattern.PatternFilterNode;
import dist.esper.epl.expr.pattern.PatternMultiChildNode;
import dist.esper.epl.expr.pattern.PatternNoChildNode;
import dist.esper.epl.expr.pattern.PatternSingleChildNode;

public abstract class AbstractExpressionVisitor<T> implements IExpressionVisitor<T>{

	@Override
	public T visitComparisionExpression(ComparisonExpression ce) {
		for(AbstractResultExpression child: ce.getChildExprList()){
			child.accept(this);
		}
		return null;
	}

	@Override
	public T visitCompositeExpression(CompositeExpression ce) {
		for(AbstractBooleanExpression child: ce.getChildExprList()){
			child.accept(this);
		}
		return null;
	}
	
	@Override
	public T visitEventIndexedSpecification(EventIndexedSpecification eis) {
		return this.visitEventSpecification(eis);
	}
	
	@Override
	public T visitEventPropertyIndexedSpecification(EventPropertyIndexedSpecification epis) {
		return this.visitEventPropertySpecification(epis);
	}

	@Override
	public T visitTimePeriod(TimePeriod t) {		
		return null;
	}

	@Override
	public T visitValue(Value v) {		
		return null;
	}

	@Override
	public T visitAggregationExpression(AggregationExpression ae) {
		ae.getExpr().accept(this);
		if(ae.getFilterExpr()!=null){
			ae.getFilterExpr().accept(this);
		}
		return null;
	}

	@Override
	public T visitMathExpression(MathExpression me) {
		for(AbstractPropertyExpression child: me.getChildExprList()){
			child.accept(this);
		}
		return null;
	}

	@Override
	public T visitUDFDotExpression(UDFDotExpression de) {
		for(UDFDotExpressionItem item: de.getItemList()){
			item.accept(this);
		}
		return null;
	}

	@Override
	public T visitPattenMultiChildNode(PatternMultiChildNode pmcn) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public T visitPattenFilterNode(PatternFilterNode pncn) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public T visitPattenSingleChildNode(PatternSingleChildNode pmcn) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public T visitUDFDotExpressionItem(UDFDotExpressionItem dei) {
		for(AbstractPropertyExpression param: dei.getParamList()){
			param.accept(this);
		}
		return null;
	}

	@Override
	public T visitWildcardExpression(WildcardExpression we) {
		return null;
	}

}
