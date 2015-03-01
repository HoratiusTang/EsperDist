package dist.esper.epl.expr;

import com.espertech.esper.client.soda.*;

public class ExpressionFactory1 {
	public static AbstractExpression toExpression1(Expression expr0){
		if(expr0==null){
			return null;
		}
		if(expr0 instanceof com.espertech.esper.client.soda.ArithmaticExpression){
			return MathExpression.Factory.make((ArithmaticExpression)expr0);
		}
		else if(expr0 instanceof com.espertech.esper.client.soda.AvgProjectionExpression ||
				expr0 instanceof com.espertech.esper.client.soda.SumProjectionExpression ||
				expr0 instanceof com.espertech.esper.client.soda.CountProjectionExpression ||
				expr0 instanceof com.espertech.esper.client.soda.CountStarProjectionExpression || 
				expr0 instanceof com.espertech.esper.client.soda.AvedevProjectionExpression ||
				expr0 instanceof com.espertech.esper.client.soda.MaxProjectionExpression ||
				expr0 instanceof com.espertech.esper.client.soda.MinProjectionExpression ||
				expr0 instanceof com.espertech.esper.client.soda.MedianProjectionExpression ||
				expr0 instanceof com.espertech.esper.client.soda.StddevProjectionExpression){
			return AggregationExpression.Factory.make(expr0);
		}
		else if(expr0 instanceof com.espertech.esper.client.soda.ConstantExpression){
			return Value.valueOf(((ConstantExpression)expr0).getConstant());
		}
		else if(expr0 instanceof com.espertech.esper.client.soda.Junction ||
				expr0 instanceof com.espertech.esper.client.soda.NotExpression){
			return CompositeExpression.Factory.make(expr0);
		}
		else if(expr0 instanceof com.espertech.esper.client.soda.RelationalOpExpression){
			return ComparisonExpression.Factory.make((RelationalOpExpression)expr0);
		}
		else if(expr0 instanceof com.espertech.esper.client.soda.TimePeriodExpression){
			return TimePeriod.Factory.make((TimePeriodExpression)expr0);
		}
		else if(expr0 instanceof com.espertech.esper.client.soda.DotExpression){
			return UDFDotExpression.Factory.make((DotExpression)expr0);
		}
		else if(expr0 instanceof com.espertech.esper.client.soda.PropertyValueExpression){
			return new UnsolvedEventOrPropertyExpression2(((PropertyValueExpression)expr0).getPropertyName());
		}
		
		throw new RuntimeException("unknown Experssion type: "+expr0.getClass().getName());
	}
}
