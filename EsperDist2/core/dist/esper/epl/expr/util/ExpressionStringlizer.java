package dist.esper.epl.expr.util;

import java.util.List;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;

public class ExpressionStringlizer extends AbstractExpressionVisitor2<StringBuilder, StringBuilder>{
	protected static ExpressionStringlizer instance=new ExpressionStringlizer();
	
	public static ExpressionStringlizer getInstance(){
		return instance;
	}
	
	public void toStringBuilder(AbstractExpression expr, StringBuilder sb){
		expr.accept(this, sb);
	}
	
	public StringBuilder toStringBuilder(List<AbstractBooleanExpression> bExprList, RelationTypeEnum relation, StringBuilder sw){
		if(bExprList.size()>0)
			sw.append("(");
		toStringBuilder(bExprList, " "+relation.toString()+" ", sw);
		if(bExprList.size()>0)
			sw.append(")");
		return sw;
	}
	
	public <E extends AbstractExpression> StringBuilder toStringBuilder(
			List<E> paramList, String dem, StringBuilder sw){
		String delimiter="";
		for(AbstractExpression param: paramList){
			sw.append(delimiter);
			delimiter=dem;
			param.accept(this, sw);
		}
		return sw;
	}
	
	@Override
	public StringBuilder visitComparisionExpression(ComparisonExpression ce,
			StringBuilder sw) {
		sw.append("(");
		ce.getChildExprList().get(0).accept(this, sw);
		sw.append(ce.getRelation().toString());
		ce.getChildExprList().get(1).accept(this, sw);
		sw.append(")");
		return sw;
	}

	@Override
	public StringBuilder visitCompositeExpression(CompositeExpression ce,
			StringBuilder sw) {
		sw.append("(");
		ce.getChildExprList().get(0).accept(this, sw);
		for(int i=1;i<ce.getChildExprList().size();i++){
			sw.append(" ");
			sw.append(ce.getRelation().toString());
			sw.append(" ");
			ce.getChildExprList().get(i).accept(this, sw);
		}
		sw.append(")");
		return sw;
	}

	@Override
	public StringBuilder visitEventSpecification(EventSpecification es,
			StringBuilder sw) {
		try{
			sw.append(es.getEventAlias().toString());
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		return sw;
	}

	@Override
	public StringBuilder visitEventIndexedSpecification(EventIndexedSpecification eis,
			StringBuilder sw) {
		this.visitEventSpecification(eis, sw);
		sw.append("["+eis.getIndex()+"]");
		return sw;
	}

	@Override
	public StringBuilder visitEventPropertySpecification(
			EventPropertySpecification eps, StringBuilder sw) {
		eps.getEventSpec().accept(this, sw);
		sw.append(".");
		sw.append(eps.getEventProp().getName());
		return null;
	}

	@Override
	public StringBuilder visitEventPropertyIndexedSpecification(
			EventPropertyIndexedSpecification epis, StringBuilder sw) {
		this.visitEventPropertySpecification(epis, sw);
		sw.append("["+epis.getIndex()+"]");
		return sw;
	}

	@Override
	public StringBuilder visitTimePeriod(TimePeriod t, StringBuilder sw) {
		int i=0;
		long[] time=t.getTime();
		while(i<time.length && time[i]<=0){
			i++;
		}
		if(i==time.length){
			sw.append("0 second");
			return sw;
		}
		else{
			String delimiter="";
			for( ;i<time.length;i++){
				if(time[i]>0){
					sw.append(delimiter);
					sw.append(time[i]+" "+TimePeriod.TIME_UNITS[i].toString());
					delimiter=" ";
				}
			}
		}
		return sw;
	}

	@Override
	public StringBuilder visitValue(Value v, StringBuilder sw) {
		sw.append(v.getValueStr());
		return null;
	}

	@Override
	public StringBuilder visitWildcardExpression(WildcardExpression we,
			StringBuilder sw) {
		sw.append(we.toString());
		return null;
	}

	@Override
	public StringBuilder visitAggregationExpression(AggregationExpression ae,
			StringBuilder sw) {
		sw.append(ae.getAggType().getName());
		sw.append('(');
		if(ae.isDistinct()){
			sw.append("distinct ");
		}
		ae.getExpr().accept(this, sw);
		if(ae.getFilterExpr()!=null){
			sw.append(',');
			ae.getFilterExpr().accept(this, sw);
		}
		sw.append(')');
		return sw;
	}

	@Override
	public StringBuilder visitMathExpression(MathExpression me, StringBuilder sw) {
		me.getChildExprList().get(0).accept(this, sw);
		for(int i=1;i<me.getChildExprList().size();i++){
			sw.append(me.getOpType().getString());
			me.getChildExprList().get(i).accept(this, sw);
		}
		return sw;
	}

	@Override
	public StringBuilder visitUDFDotExpressionItem(UDFDotExpressionItem dei,
			StringBuilder sw) {
		sw.append(dei.getName());
		if(dei.isProperty()){
			return sw;
		}
		else{
			sw.append('(');
			this.toStringBuilder(dei.getParamList(), ", ", sw);
			sw.append(')');
		}
		return sw;
	}

	@Override
	public StringBuilder visitUDFDotExpression(UDFDotExpression de, StringBuilder sw) {
		this.toStringBuilder(de.getItemList(), ".", sw);
		return sw;
	}
}
