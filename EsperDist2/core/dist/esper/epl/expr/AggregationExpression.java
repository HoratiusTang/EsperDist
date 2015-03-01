package dist.esper.epl.expr;


import com.espertech.esper.client.soda.Expression;
import com.espertech.esper.epl.expression.ExprAggregateNode;
import com.espertech.esper.epl.expression.ExprAvedevNode;
import com.espertech.esper.epl.expression.ExprAvgNode;
import com.espertech.esper.epl.expression.ExprConstantNode;
import com.espertech.esper.epl.expression.ExprCountNode;
import com.espertech.esper.epl.expression.ExprFirstEverNode;
import com.espertech.esper.epl.expression.ExprIdentNode;
import com.espertech.esper.epl.expression.ExprLastEverNode;
import com.espertech.esper.epl.expression.ExprLeavingAggNode;
import com.espertech.esper.epl.expression.ExprMedianNode;
import com.espertech.esper.epl.expression.ExprMinMaxAggrNode;
import com.espertech.esper.epl.expression.ExprNode;
import com.espertech.esper.epl.expression.ExprNthAggNode;
import com.espertech.esper.epl.expression.ExprRateAggNode;
import com.espertech.esper.epl.expression.ExprStddevNode;
import com.espertech.esper.epl.expression.ExprSumNode;
import com.espertech.esper.type.MinMaxTypeEnum;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.AggJsonSerializer.class)
public class AggregationExpression extends AbstractResultExpression{	
	private static final long serialVersionUID = -2322803666056952755L;
	AggregationEnum aggType=AggregationEnum.NONE;
	AbstractResultExpression expr=null;
	AbstractBooleanExpression filterExpr=null;
	boolean distinct;
	
	public AggregationExpression() {
		super();
	}

	public AggregationExpression(AggregationEnum agg) {
		super();
		this.aggType = agg;
	}

	public AggregationExpression(AggregationEnum agg, AbstractResultExpression expr,
			AbstractBooleanExpression filterExpr) {
		super();
		this.aggType = agg;
		this.expr = expr;
		this.filterExpr = filterExpr;
	}

	public AbstractResultExpression getExpr() {
		return expr;
	}

	public void setExpr(AbstractResultExpression expr) {
		this.expr = expr;
	}

	public AbstractBooleanExpression getFilterExpr() {
		return filterExpr;
	}

	public void setFilterExpr(AbstractBooleanExpression filterExpr) {
		this.filterExpr = filterExpr;
	}
	
	public AggregationEnum getAggType() {
		return aggType;
	}

	public void setAggType(AggregationEnum agg) {
		this.aggType = agg;
	}

	public boolean isDistinct() {
		return distinct;
	}
	
	public boolean getDistinct() {
		return distinct;
	}

	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}

	public static class Factory{
		public static AggregationExpression make(ExprAggregateNode aggNode){
			AggregationEnum aggType=AggregationEnum.NONE;
			if(aggNode instanceof ExprAvedevNode){
				aggType=AggregationEnum.AVEDEV;
			}
			else if(aggNode instanceof ExprAvgNode){
				aggType=AggregationEnum.AVG;
			}
			else if(aggNode instanceof ExprCountNode){
				aggType=AggregationEnum.COUNT;				
			}
			else if(aggNode instanceof ExprFirstEverNode){
				
			}
			else if(aggNode instanceof ExprLastEverNode){
				
			}
			else if(aggNode instanceof ExprLeavingAggNode){
				
			}
			else if(aggNode instanceof ExprMedianNode){
				aggType=AggregationEnum.MEDIAN;
			}
			else if(aggNode instanceof ExprMinMaxAggrNode){
				ExprMinMaxAggrNode minMaxNode=(ExprMinMaxAggrNode)aggNode;
				if(minMaxNode.getMinMaxTypeEnum()==MinMaxTypeEnum.MAX){
					aggType=AggregationEnum.MAX;
				}
				else{
					aggType=AggregationEnum.MIN;
				}
			}
			else if(aggNode instanceof ExprNthAggNode){
				
			}
			else if(aggNode instanceof ExprRateAggNode){
				aggType=AggregationEnum.RATE;
			}
			else if(aggNode instanceof ExprStddevNode){
				aggType=AggregationEnum.STDDEV;
			}
			else if(aggNode instanceof ExprSumNode){
				aggType=AggregationEnum.SUM;
			}
			if(aggType==AggregationEnum.NONE){
				System.err.format("Unsupported AggregationNode: %s\n", aggNode.getClass().toString());
			}
			else{
				ExprNode[] childNodes=aggNode.getChildNodes();
				AbstractResultExpression expr=null;
				AbstractBooleanExpression filterExpr=null;
				
				if(childNodes!=null && childNodes.length>0){
					if(aggType==AggregationEnum.COUNT && 
							!(childNodes[0] instanceof ExprIdentNode) &&
							!(childNodes[0] instanceof ExprConstantNode)){
						expr=WildcardExpression.getInstance();
						filterExpr=(AbstractBooleanExpression)(ExpressionFactory.toExpression(childNodes[0]));
					}
					else{
						//System.out.println(childNodes[0].toString());
						expr=(AbstractResultExpression)(ExpressionFactory.toExpression(childNodes[0]));
						if(childNodes.length>1){
							filterExpr=(AbstractBooleanExpression)(ExpressionFactory.toExpression(childNodes[1]));
						}
					}
				}
				
				if(expr==null){
					expr=WildcardExpression.getInstance();
				}
				
				AggregationExpression a=new AggregationExpression(aggType);
				a.setExpr(expr);
				a.setFilterExpr(filterExpr);
				a.setDistinct(aggNode.isDistinct());
				return a;
			}
			
			return null;
		}
		
		public static AggregationExpression make(Expression expr0){
			AggregationEnum aggType=getAggType(expr0);
			AggregationExpression ae=new AggregationExpression(aggType);
			//FIXME: distinct
			if(expr0.getChildren().size()>0){
				AbstractResultExpression expr=(AbstractResultExpression)ExpressionFactory1.toExpression1(expr0.getChildren().get(0));
				ae.setExpr(expr);
			}
			else{
				assert(aggType==AggregationEnum.COUNTSTAR);
				ae.setExpr(WildcardExpression.getInstance());
			}
			
			if(expr0.getChildren().size()>1){
				AbstractBooleanExpression filterExpr=(AbstractBooleanExpression)ExpressionFactory1.toExpression1(expr0.getChildren().get(1));
				ae.setFilterExpr(filterExpr);
			}
			return ae;//TODO: make
		}
		
		private static AggregationEnum getAggType(Expression expr0){
			if(expr0 instanceof com.espertech.esper.client.soda.AvgProjectionExpression){
				return AggregationEnum.AVG;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.SumProjectionExpression){
				return AggregationEnum.SUM;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.CountProjectionExpression){
				return AggregationEnum.COUNT;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.CountStarProjectionExpression){
				return AggregationEnum.COUNTSTAR;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.AvedevProjectionExpression){
				return AggregationEnum.AVEDEV;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.MaxProjectionExpression){
				return AggregationEnum.MAX;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.MinProjectionExpression){
				return AggregationEnum.MIN;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.MedianProjectionExpression){
				return AggregationEnum.MEDIAN;
			}
			else if(expr0 instanceof com.espertech.esper.client.soda.StddevProjectionExpression){
				return AggregationEnum.STDDEV;
			}
			return AggregationEnum.NONE;
		}
	}

	/**
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(aggType.getName());
		sw.append('(');
		if(distinct){
			sw.append("distinct ");
		}
		this.expr.toStringBuilder(sw);		
		if(this.filterExpr!=null){
			sw.append(',');
			this.filterExpr.toStringBuilder(sw);
		}
		sw.append(')');
	}
	*/
	/**
	@Override
	public void toStringBuilderWithNickName(StringBuilder sw) {
		sw.append(aggType.getName());
		sw.append('(');
		if(distinct){
			sw.append("distinct ");
		}
		this.expr.toStringBuilderWithNickName(sw);
		if(this.filterExpr!=null){
			sw.append(',');
			this.filterExpr.toStringBuilderWithNickName(sw);
		}
		sw.append(')');
	}
	**/
	
	/**
	@Override
	public int eigenCode() {
		int code = aggType.ordinal() ^ (distinct?1231:1237) ^ expr.eigenCode();
		code ^= filterExpr.eigenCode();
		return code;
	}
	*/

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		if(!this.expr.resolve(ssw,null)){
			this.setExpr((AbstractResultExpression)ExpressionFactory.resolve(expr, ssw, param));
		}
		if(this.filterExpr!=null){
			this.filterExpr.resolve(ssw,null);
		}
		return true;
	}

	/**
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		expr.dumpAllEventAliases(eaSet);
		if(filterExpr!=null){
			filterExpr.dumpAllEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		expr.dumpAllEventOrPropertySpecReferences(epsSet);
		if(filterExpr!=null){
			filterExpr.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	*/
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitAggregationExpression(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitAggregationExpression(this, obj);
	}
}
