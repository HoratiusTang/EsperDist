package dist.esper.core.flow.stream;

import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResult;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.util.ExpressionStringlizer;
import dist.esper.util.StringUtil;

/**
 * the factory class to print all kinds @Stream(s)
 * @author tjy
 *
 */
public class StreamStringlizer {
	public static void toStringBuilder(FilterStream fsl, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(fsl.getClass().getSimpleName());
		if(fsl.getWorkerId()!=null){
			sw.append("["+fsl.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(fsl.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(fsl.getResultElementList().toString());
		if(fsl.getEventSpec()!=null){
			sw.append(" : ");
			fsl.getEventSpec().toStringBuilder(sw);
		}
		sw.append("(");
		fsl.getFilterExpr().toStringBuilder(sw);
		sw.append(")");
		if(fsl.getFinalReusingContainerMapComparisonResult()!=null){
			sw.append(" -- ");
			ContainerAndMapAndBoolComparisonResult fcmcr=fsl.getFinalReusingContainerMapComparisonResult();
			sw.append("("+fcmcr.getFirst().getId()+")");
			/*AbstractBooleanExpression.toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);*/
			ExpressionStringlizer.getInstance().toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);
		}
		sw.append("\n");
		if(fsl.getRawStream()!=null){
			fsl.getRawStream().toStringBuilder(sw, indent+4);
		}
		//sw.append("\n");
	}
	
	public static void toStringBuilder(FilterDelayedStream fcsl, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(fcsl.getClass().getSimpleName());
		if(fcsl.getWorkerId()!=null){
			sw.append("["+fcsl.getWorkerId().id+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(fcsl.getWindowTimeViewSpecString());
		sw.append(" : ");		
		sw.append(fcsl.getResultElementList().toString());
		if(fcsl.getEventSpec()!=null){
			sw.append(" : ");
			fcsl.getEventSpec().toStringBuilder(sw);
		}
//		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(extraFilterCondList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(fcsl.getExtraFilterCondList(), RelationTypeEnum.AND, sw);
//		sw.append(")");
		if(fcsl.getFinalReusingContainerMapComparisonResult()!=null){
			sw.append(" -- ");
			ContainerAndMapAndBoolComparisonResult fcmcr=fcsl.getFinalReusingContainerMapComparisonResult();
			sw.append("("+fcmcr.getFirst().getId()+")");
			/*AbstractBooleanExpression.toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);*/
			ExpressionStringlizer.getInstance().toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);
		}
		sw.append("\n");
		fcsl.getAgent().toStringBuilder(sw, indent+4);
		//sw.append("\n");
	}
	
	public static void toStringBuilder(JoinStream jsl, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(jsl.getClass().getSimpleName());
		if(jsl.getWorkerId()!=null){
			sw.append("["+jsl.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(jsl.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(jsl.getResultElementList().toString());
		sw.append(" : ");
//		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(joinExprList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(jsl.getJoinExprList(), RelationTypeEnum.AND, sw);
//		sw.append(")");
		if(jsl.getFinalReusingContainerMapComparisonResult()!=null){
			sw.append(" -- ");
			ContainerAndMapAndBoolComparisonResult fcmcr=jsl.getFinalReusingContainerMapComparisonResult();
			sw.append("("+fcmcr.getFirst().getId()+")");
			/*AbstractBooleanExpression.toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);*/
			ExpressionStringlizer.getInstance().toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);
		}
		sw.append("\n");
		for(Stream child: jsl.getUpStreamList()){
			child.toStringBuilder(sw, indent+4);
			if(!(child instanceof JoinStream)){
				sw.append("\n");
			}
		}
	}
	public static void toStringBuilder(JoinDelayedStream jcsl, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(jcsl.getClass().getSimpleName());
		if(jcsl.getWorkerId()!=null){
			sw.append("["+jcsl.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(" : ");
		sw.append(jcsl.getResultElementList().toString());
		sw.append(" : ");
		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(extraJoinCondList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(jcsl.getExtraJoinCondList(), RelationTypeEnum.AND, sw);
		sw.append(" and ");
		/*AbstractBooleanExpression.toStringBuilder(extraChildCondList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(jcsl.getExtraChildCondList(), RelationTypeEnum.AND, sw);
		sw.append(")");
		if(jcsl.getFinalReusingContainerMapComparisonResult()!=null){
			sw.append(" -- ");
			ContainerAndMapAndBoolComparisonResult fcmcr=jcsl.getFinalReusingContainerMapComparisonResult();
			sw.append("("+fcmcr.getFirst().getId()+")");
			/*AbstractBooleanExpression.toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);*/
			ExpressionStringlizer.getInstance().toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);
		}
		sw.append("\n");
		jcsl.getAgent().toStringBuilder(sw, indent+4);
	}
	
	public static void toStringBuilder(RootStream rsl, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(rsl.getClass().getSimpleName());
		if(rsl.getWorkerId()!=null){
			sw.append("["+rsl.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(rsl.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(rsl.getResultElementList().toString());
		sw.append(" : ");
//		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(whereExprList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(rsl.getWhereExprList(), RelationTypeEnum.AND, sw);
//		sw.append(")");
		if(rsl.getFinalReusingContainerMapComparisonResult()!=null){
			sw.append(" -- ");
			ContainerAndMapAndBoolComparisonResult fcmcr=rsl.getFinalReusingContainerMapComparisonResult();
			sw.append("("+fcmcr.getFirst().getId()+")");
			/*AbstractBooleanExpression.toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);*/
			ExpressionStringlizer.getInstance().toStringBuilder(fcmcr.getFirst().dumpOwnBooleanExpressions(), RelationTypeEnum.AND, sw);
		}
		sw.append("\n");
		rsl.getUpStream().toStringBuilder(sw, indent+4);
	}
	
	public static void toStringBuilder(RawStream rsl, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(rsl.getClass().getSimpleName());
		if(rsl.getWorkerId()!=null){
			sw.append("["+rsl.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(" : ");
		sw.append(rsl.event.getName());
	}
	
	public static void toStringBuilder(PatternStream psl, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(psl.getClass().getSimpleName());
		if(psl.workerId!=null){
			sw.append("["+psl.workerId.id+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(" : ");
		sw.append(psl.getResultElementList().toString());
		if(psl.getPatternNode()!=null){
			sw.append(" : ");
			psl.getPatternNode().toStringBuilder(sw);
		}
		sw.append("\n");
		for(RawStream node: psl.getRawStreamList()){
			node.toStringBuilder(sw, indent+4);
			sw.append("\n");
		}
	}
	
}
