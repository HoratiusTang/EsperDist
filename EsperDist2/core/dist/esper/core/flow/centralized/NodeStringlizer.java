package dist.esper.core.flow.centralized;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.util.StringUtil;

/**
 * the factory class to print all kinds of @Node(s)
 * @author tjy
 *
 */
public class NodeStringlizer {
	public static void toStringBuilder(FilterNode fn, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(fn.getClass().getSimpleName());
		sw.append(" : ");
		sw.append(fn.getResultElementList().toString());
		sw.append(" : ");
		fn.getEventSpec().toStringBuilder(sw);
		sw.append("(");
//		String delimiter="";
//		for(AbstractBooleanExpression filterExpr: filterExprList){
//			sw.append(delimiter);
//			filterExpr.toStringBuilder(sw);
//			delimiter=" and ";
//		}
		fn.getFilterExpr().toStringBuilder(sw);
		sw.append(")");
	}
	
	public static void toStringBuilder(PatternNode pn, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(pn.getClass().getSimpleName());
		sw.append(" : ");
		sw.append(pn.getResultElementList().toString());
		sw.append(" : ");
		pn.getPatternNode().toStringBuilder(sw);
	}
	
	public static void toStringBuilder(JoinNode jn, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(jn.getClass().getSimpleName());		
		sw.append(" : ");
		sw.append(jn.getResultElementList().toString());
		sw.append(" : ");
		sw.append("(");
		String delimiter="";
		for(AbstractBooleanExpression joinExpr: jn.getJoinExprList()){
			sw.append(delimiter);
			joinExpr.toStringBuilder(sw);
			delimiter=" and ";
		}
		sw.append(")\n");
		for(Node node: jn.getChildList()){
			node.toStringBuilder(sw, indent+4);
			if(!(node instanceof JoinNode)){
				sw.append("\n");
			}
		}	
	}
	
	public static void toStringBuilder(RootNode rn, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(rn.getClass().getSimpleName());
		sw.append(" : ");
		sw.append(rn.getResultElementList().toString());
		sw.append(" : ");		
		sw.append("(");
		String delimiter="";
		for(AbstractBooleanExpression filterExpr: rn.getWhereExprList()){
			sw.append(delimiter);
			filterExpr.toStringBuilder(sw);
			delimiter=" and ";
		}
		sw.append(")");
		sw.append("\n");
		rn.getChild().toStringBuilder(sw, indent+4);
	}
}
