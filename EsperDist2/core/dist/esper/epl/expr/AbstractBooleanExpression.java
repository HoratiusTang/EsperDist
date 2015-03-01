package dist.esper.epl.expr;


import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBooleanExpression extends AbstractPropertyExpression {	
	private static final long serialVersionUID = 6194419142831644684L;

	public List<AbstractBooleanExpression> dumpConjunctionExpressions(){
		List<AbstractBooleanExpression> conjuntExprList=new ArrayList<AbstractBooleanExpression>(4);
		dumpConjunctionExpressions(conjuntExprList);
		return conjuntExprList;
	}
	public abstract void dumpConjunctionExpressions(List<AbstractBooleanExpression> conjunctList);
	
	public static int getComparisonExpressionCount(List<AbstractBooleanExpression> bExprList){
		int comparisonCount=0;
		for(AbstractBooleanExpression bExpr: bExprList){
			comparisonCount += bExpr.getComparisonExpressionCount();
		}
		return comparisonCount;
	}
	public abstract int getComparisonExpressionCount();
	
	/**
	public static void toStringBuilder(List<AbstractBooleanExpression> bExprList, RelationTypeEnum relation, StringBuilder sw){
		sw.append("(");
		String delimiter="";
		String relationDelimiter=" "+relation.toString()+" ";		
		for(int i=0;i<bExprList.size();i++){
			sw.append(delimiter);
			bExprList.get(i).toStringBuilder(sw);
			delimiter=relationDelimiter;
		}
		sw.append(")");
	}
	
	public static void toStringBuilderWithNickName(List<AbstractBooleanExpression> bExprList, RelationTypeEnum relation, StringBuilder sw){
		sw.append("(");
		String delimiter="";
		String relationDelimiter=" "+relation.toString()+" ";		
		for(int i=0;i<bExprList.size();i++){
			sw.append(delimiter);
			bExprList.get(i).toStringBuilderWithNickName(sw);
			delimiter=relationDelimiter;
		}
		sw.append(")");
	}
	*/
}
