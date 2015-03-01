package dist.esper.core.flow.stream;

import java.util.*;

import dist.esper.core.flow.container.DerivedStreamContainer.StreamAndMapAndBoolComparisonResult;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.BooleanExpressionComparisonPair;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;

public class StreamFactory {
	public static StreamAndMapAndBoolComparisonResult genContainerMapComparisonResult(
			DerivedStream pcsl){
		Set<EventAlias> eaSet=pcsl.dumpEventAlias();
		Map<EventAlias,EventAlias> eaMap=new HashMap<EventAlias,EventAlias>();
		for(EventAlias ea: eaSet){
			eaMap.put(ea, ea);
		}
		List<AbstractBooleanExpression> ownCondList=pcsl.dumpCurrentBooleanExpressions();
		List<AbstractBooleanExpression> childrenCondList=pcsl.dumpChildrenBooleanExpressions();
		
		BooleanExpressionComparisonResult cr=new BooleanExpressionComparisonResult();
		for(AbstractBooleanExpression ownCond: ownCondList){
			cr.addOwnPair(new BooleanExpressionComparisonPair(ownCond, ownCond, State.EQUIVALENT));
		}
		for(AbstractBooleanExpression childCond: childrenCondList){
			cr.addChildPair(new BooleanExpressionComparisonPair(childCond, childCond, State.EQUIVALENT));
		}
		return new StreamAndMapAndBoolComparisonResult(pcsl, eaMap, cr);
	}
}
