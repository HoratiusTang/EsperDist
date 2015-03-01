package dist.esper.epl.expr;

import com.espertech.esper.filter.*;

public class FilterFactory {
	public static AbstractBooleanExpression toBooleanExpression(FilterSpecParam fsp, String eventAsName){
		if(fsp instanceof FilterSpecParamConstant){
			FilterSpecParamConstant fspc=(FilterSpecParamConstant)fsp;
			ComparisonExpression ce=new ComparisonExpression(toOperatorTypeEnum(fsp.getFilterOperator()));
			UnsolvedEventOrPropertyExpression left=new UnsolvedEventOrPropertyExpression(
					eventAsName,
					fspc.getLookupable().getExpression(),
					null,null);
			Value right=new Value(fspc.getFilterConstant());
			ce.addExpression(left);
			ce.addExpression(right);
			
			return ce;
		}
		else if(fsp instanceof FilterSpecParamEventProp){
			FilterSpecParamEventProp fspep=(FilterSpecParamEventProp)fsp;
			ComparisonExpression ce=new ComparisonExpression(toOperatorTypeEnum(fsp.getFilterOperator()));
			UnsolvedEventOrPropertyExpression left=new UnsolvedEventOrPropertyExpression(
					eventAsName,
					fspep.getLookupable().getExpression(),
					null,null);
			UnsolvedEventOrPropertyExpression right=new UnsolvedEventOrPropertyExpression(
					fspep.getResultEventAsName(),
					fspep.getResultEventProperty(),
					null,null);
			ce.addExpression(left);
			ce.addExpression(right);
			return ce;
		}
		else if(fsp instanceof FilterSpecParamEventPropIndexed){
			FilterSpecParamEventPropIndexed fspep=(FilterSpecParamEventPropIndexed)fsp;
			ComparisonExpression ce=new ComparisonExpression(toOperatorTypeEnum(fsp.getFilterOperator()));
			UnsolvedEventOrPropertyExpression left=new UnsolvedEventOrPropertyExpression(
					eventAsName,
					fspep.getLookupable().getExpression(),
					null,null);
			UnsolvedEventOrPropertyExpression right=new UnsolvedEventOrPropertyExpression(
					null,null,null,
					fspep.getResultEventAsName()+"["+fspep.getResultEventIndex()+"]."+
					fspep.getResultEventProperty()
					);
			ce.addExpression(left);
			ce.addExpression(right);
			return ce;
		}
		else if(fsp instanceof FilterSpecParamExprNode){
			FilterSpecParamExprNode fspen=(FilterSpecParamExprNode)fsp;
			return (AbstractBooleanExpression)(ExpressionFactory.toExpression(fspen.getExprNode()));
		}
		System.err.format("Unsupported FilterSpecParam: %s\n", fsp.getClass().toString());
		return null;
	}
	
	public static AbstractBooleanExpression toBooleanExpression(FilterSpecParam[] fsps, String eventAsName){
		if(fsps==null || fsps.length==0){
			return null;
		}
		if(fsps.length<=1){
			AbstractBooleanExpression be=toBooleanExpression(fsps[0],eventAsName);
			return be;
		}
		else{
			CompositeExpression ce=new CompositeExpression(RelationTypeEnum.AND);
			for(FilterSpecParam fsp: fsps){
				AbstractBooleanExpression be=toBooleanExpression(fsp,eventAsName);
				ce.addExpression(be);
			}
			return ce;
		}
	}
	
	public static OperatorTypeEnum toOperatorTypeEnum(FilterOperator fop){
		switch(fop){
			case EQUAL:
				return OperatorTypeEnum.EQUAL;
			case NOT_EQUAL:
				return OperatorTypeEnum.NOT_EQUAL;
//			case IS:
//				return OperatorTypeEnum.IS;
//			case IS_NOT:
//				return OperatorTypeEnum.IS_NOT;
			case LESS:
				return OperatorTypeEnum.LESS;
			case LESS_OR_EQUAL:
				return OperatorTypeEnum.LESS_OR_EQUAL;
			case GREATER_OR_EQUAL:
				return OperatorTypeEnum.GREATER_OR_EQUAL;
			case GREATER:
				return OperatorTypeEnum.GREATER;
		}
		return OperatorTypeEnum.NONE;
	}
}
