package dist.esper.core.flow.container;

import dist.esper.core.flow.stream.JoinStream;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.util.ExpressionStringlizer;
import dist.esper.util.StringUtil;

/**
 * the factory class to print all kinds @StreamContainer(s)
 * @author tjy
 *
 */
public class ContainerStringlizer {
	public static void toStringBuilder(FilterStreamContainer fsc, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(fsc.getClass().getSimpleName());
		sw.append(String.format("(%d: %d-%s-%s)", fsc.getId(), fsc.getEplId(), 
				fsc.getDirectReuseStreamMapComparisonResultEplIdList().toString(),
				fsc.getIndirectReuseStreamMapComparisonResultEplIdList().toString()));
		if(fsc.getWorkerId()!=null){
			sw.append("["+fsc.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(fsc.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(fsc.getResultElementList().toString());
		if(fsc.getEventSpec()!=null){
			sw.append(" : ");
			fsc.getEventSpec().toStringBuilder(sw);
		}
		sw.append("(");

		fsc.getFilterExpr().toStringBuilder(sw);
		sw.append(")");
		sw.append("\n");
		if(fsc.getRawStream()!=null){
			fsc.getRawStream().toStringBuilder(sw, indent+4);
		}
		//sw.append("\n");
	}
	public static void toStringBuilder(FilterDelayedStreamContainer fcsc, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(fcsc.getClass().getSimpleName());
		sw.append(String.format("(%d: %d-%s-%s)", fcsc.getId(), fcsc.getEplId(), 
				fcsc.getDirectReuseStreamMapComparisonResultEplIdList().toString(),
				fcsc.getIndirectReuseStreamMapComparisonResultEplIdList().toString()));
		if(fcsc.getWorkerId()!=null){
			sw.append("["+fcsc.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(fcsc.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(fcsc.getResultElementList().toString());
		if(fcsc.getEventSpec()!=null){
			sw.append(" : ");
			fcsc.getEventSpec().toStringBuilder(sw);
		}
//		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(extraFilterCondList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(fcsc.getExtraFilterCondList(), RelationTypeEnum.AND, sw);
//		sw.append(")");
		sw.append("\n");
		fcsc.getAgent().toStringBuilder(sw, indent+4);
		//sw.append("\n");
	}
	public static void toStringBuilder(JoinStreamContainer jsc, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(jsc.getClass().getSimpleName());
		sw.append(String.format("(%d: %d-%s-%s)", jsc.getId(), jsc.getEplId(), 
				jsc.getDirectReuseStreamMapComparisonResultEplIdList().toString(),
				jsc.getIndirectReuseStreamMapComparisonResultEplIdList().toString()));
		if(jsc.getWorkerId()!=null){
			sw.append("["+jsc.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(jsc.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(jsc.getResultElementList().toString());
		sw.append(" : ");
//		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(joinExprList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(jsc.getJoinExprList(), RelationTypeEnum.AND, sw);
//		sw.append(")");
		sw.append("\n");
		for(Stream child: jsc.getUpContainerList()){
			child.toStringBuilder(sw, indent+4);
			if(!(child instanceof JoinStream)){
				sw.append("\n");
			}
		}
	}
	public static void toStringBuilder(JoinDelayedStreamContainer jcsc, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(jcsc.getClass().getSimpleName());
		sw.append(String.format("(%d: %d-%s-%s)", jcsc.getId(), jcsc.getEplId(), 
				jcsc.getDirectReuseStreamMapComparisonResultEplIdList().toString(),
				jcsc.getIndirectReuseStreamMapComparisonResultEplIdList().toString()));
		if(jcsc.getWorkerId()!=null){
			sw.append("["+jcsc.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(jcsc.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(jcsc.getResultElementList().toString());
		sw.append(" : ");
		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(jcsc.extraJoinCondList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(jcsc.extraJoinCondList, RelationTypeEnum.AND, sw);
		sw.append(" and ");
		/*AbstractBooleanExpression.toStringBuilder(jcsc.extraChildCondList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(jcsc.extraChildCondList, RelationTypeEnum.AND, sw);
		sw.append(")");
		sw.append("\n");
		jcsc.getAgent().toStringBuilder(sw, indent+4);
	}
	public static void toStringBuilder(RootStreamContainer rsc, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(rsc.getClass().getSimpleName());
		sw.append(String.format("(%d: %d-%s-%s)", rsc.getId(), rsc.getEplId(), 
				rsc.getDirectReuseStreamMapComparisonResultEplIdList().toString(),
				rsc.getIndirectReuseStreamMapComparisonResultEplIdList().toString()));
		if(rsc.getWorkerId()!=null){
			sw.append("["+rsc.getWorkerId().getId()+"]");
		}
		else{
			sw.append("[no worker]");
		}
		sw.append(rsc.getWindowTimeViewSpecString());
		sw.append(" : ");
		sw.append(rsc.getResultElementList().toString());
		sw.append(" : ");
//		sw.append("(");
		/*AbstractBooleanExpression.toStringBuilder(whereExprList, RelationTypeEnum.AND, sw);*/
		ExpressionStringlizer.getInstance().toStringBuilder(rsc.getWhereExprList(), RelationTypeEnum.AND, sw);
//		sw.append(")");		
		sw.append("\n");
		rsc.getUpContainer().toStringBuilder(sw, indent+4);
	}
	
	public static void toStringBuilder(PatternStreamContainer psc, StringBuilder sw, int indent){
		sw.append(StringUtil.getSpaces(indent));
		sw.append(psc.getClass().getSimpleName());
		sw.append(String.format("(%d: %d-%s-%s)", psc.getId(), psc.getEplId(), 
				psc.getDirectReuseStreamMapComparisonResultEplIdList().toString(),
				psc.getIndirectReuseStreamMapComparisonResultEplIdList().toString()));
		if(psc.getWorkerId()!=null){
			sw.append("["+psc.getWorkerId().getId()+"]");
		}
		sw.append(" : ");
		sw.append(psc.getResultElementList().toString());
		if(psc.getPatternNode()!=null){
			sw.append(" : ");
			psc.getPatternNode().toStringBuilder(sw);
		}
		sw.append("\n");
		for(RawStream node: psc.getRawStreamList()){
			node.toStringBuilder(sw, indent+4);
			sw.append("\n");
		}
	}
}
