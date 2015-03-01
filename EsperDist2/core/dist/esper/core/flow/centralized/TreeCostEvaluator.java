package dist.esper.core.flow.centralized;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.AbstractResultExpression;
import dist.esper.epl.expr.ComparisonExpression;
import dist.esper.epl.expr.CompositeExpression;
import dist.esper.epl.expr.DataTypeEnum;
import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.epl.expr.RelationTypeEnum;
import dist.esper.epl.expr.Value;
import dist.esper.util.Tuple3D;

/**
 * the cost evaluator for @Tree.
 * simply count in the where conditions, 
 * without any priori knowledge of the stream data.
 *  
 * @author tjy
 *
 */
class TreeCostEvaluator{
	IdentityHashMap<AbstractBooleanExpression, Double> condCostMap=new IdentityHashMap<AbstractBooleanExpression, Double>();
	
	static final double CARTESIAN_PRODUCT_COST=100.0d;
	static final double DEFAULT_COST=30.0d;
	static final CostTuple[] costTuples={
			new CostTuple(DataTypeEnum.INT, OperatorTypeEnum.EQUAL, 2.0d),
			new CostTuple(DataTypeEnum.INT, OperatorTypeEnum.LESS, 20.0d),
			new CostTuple(DataTypeEnum.INT, OperatorTypeEnum.LESS_OR_EQUAL, 20.0d),
			new CostTuple(DataTypeEnum.INT, OperatorTypeEnum.GREATER_OR_EQUAL, 20.0d),
			new CostTuple(DataTypeEnum.INT, OperatorTypeEnum.GREATER, 20.0d),
			new CostTuple(DataTypeEnum.INT, OperatorTypeEnum.NOT_EQUAL, 50.0d),
			
			new CostTuple(DataTypeEnum.DOUBLE, OperatorTypeEnum.EQUAL, 1.0d),
			new CostTuple(DataTypeEnum.DOUBLE, OperatorTypeEnum.LESS, 20.0d),
			new CostTuple(DataTypeEnum.DOUBLE, OperatorTypeEnum.LESS_OR_EQUAL, 20.0d),
			new CostTuple(DataTypeEnum.DOUBLE, OperatorTypeEnum.GREATER_OR_EQUAL, 20.0d),
			new CostTuple(DataTypeEnum.DOUBLE, OperatorTypeEnum.GREATER, 20.0d),
			new CostTuple(DataTypeEnum.DOUBLE, OperatorTypeEnum.NOT_EQUAL, 50.0d),
			
			new CostTuple(DataTypeEnum.STRING, OperatorTypeEnum.EQUAL, 1.0d),
			new CostTuple(DataTypeEnum.STRING, OperatorTypeEnum.LESS, 20.0d),
			new CostTuple(DataTypeEnum.STRING, OperatorTypeEnum.LESS_OR_EQUAL, 20.0d),
			new CostTuple(DataTypeEnum.STRING, OperatorTypeEnum.GREATER_OR_EQUAL, 20.0d),
			new CostTuple(DataTypeEnum.STRING, OperatorTypeEnum.GREATER, 20.0d),
			new CostTuple(DataTypeEnum.STRING, OperatorTypeEnum.NOT_EQUAL, 50.0d)
	};

	public double evalTree(Tree tree){
		double cost=evalRootNode(tree.getRoot());
		tree.setRoughCost(cost);
		return cost;
	}		
	public double evalNode(Node node){
		if(node instanceof RootNode){
			return evalRootNode((RootNode)node);
		}
		else if(node instanceof JoinNode){
			return evalJoinNode((JoinNode)node);
		}
		else if(node instanceof FilterNode){
			return evalFilterNode((FilterNode)node);
		}
		else if(node instanceof PatternNode){
			return evalPatternNode((PatternNode)node);
		}
		return 0.0d;
	}		
	public double evalRootNode(RootNode rn){
		return evalNode(rn.getChild());
	}
	private double evalJoinNode(JoinNode jn){
		double cost=0.0d;
		if(jn.getJoinExprList().size()>0)
			cost+=evalBooleanExpressionList(jn.getJoinExprList());
		else
			cost+=CARTESIAN_PRODUCT_COST;
		
		cost = cost/jn.getLevel();
		for(Node child: jn.getChildList()){
			cost+=evalNode(child);
		}
		return cost;
	}
	private double evalFilterNode(FilterNode fn){
		return evalBooleanExpression(fn.getFilterExpr());
	}		
	private double evalPatternNode(PatternNode pn){
		throw new RuntimeException("not implemented yet");
	}
	
	public double evalBooleanExpression(AbstractBooleanExpression bExpr){
		Double cost=condCostMap.get(bExpr);
		if(cost==null){
			if(bExpr instanceof ComparisonExpression){
				cost=evalComparisonExpression((ComparisonExpression)bExpr);
			}
			else{
				cost=evalCompositeExpression((CompositeExpression)bExpr);
			}
			condCostMap.put(bExpr, cost);
		}
		return cost.doubleValue();
	}
	
	public double evalBooleanExpressionList(List<AbstractBooleanExpression> bExprList){		
		return evalCompositeExpression(new CompositeExpression(bExprList));
	}
	
	private double evalComparisonExpression(ComparisonExpression ce){
		DataTypeEnum[] dts=new DataTypeEnum[ce.getChildExprList().size()];
		for(int i=0;i<ce.getChildExprList().size();i++){
			dts[i]=getDataType(ce.getChild(i));
		}
		DataTypeEnum dt=DataTypeEnum.INT;
		for(int i=0;i<dts.length;i++){
			if(dts[i]!=null && dts[i].ordinal()>dt.ordinal()){//FIXME: NOT so strict
				dt=dts[i];
			}
		}
		dt=(dt==DataTypeEnum.FLOAT)?DataTypeEnum.DOUBLE:dt;
		return estimateCost(dt, ce.getRelation());
	}
	
	private double estimateCost(DataTypeEnum dt, OperatorTypeEnum op){
		for(CostTuple t: costTuples){
			if(t.getFirst()==dt && t.getSecond()==op){
				return t.getThird();
			}
		}
		return DEFAULT_COST;
	}
	
	private DataTypeEnum getDataType(AbstractResultExpression expr){
		if(expr instanceof Value){
			return ((Value)expr).getType();
		}
		//FIXME
		return DataTypeEnum.INT;
	}
	
	private double evalCompositeExpression(CompositeExpression ce){
		double[] childCosts=new double[ce.getChildExprList().size()];
		double childMinCost=Double.MAX_VALUE;
		double childMaxCost=Double.MIN_VALUE;
		for(int i=0;i<ce.getChildExprCount();i++){
			childCosts[i]=evalBooleanExpression(ce.getChildExprList().get(i));
			childMinCost=(childCosts[i]<childMinCost)?childCosts[i]:childMinCost;
			childMaxCost=(childCosts[i]>childMaxCost)?childCosts[i]:childMaxCost;
		}
		if(ce.getRelation()==RelationTypeEnum.AND){
			double cost=childMaxCost/ce.getChildExprCount();
			cost=(childMinCost<cost)?childMinCost:cost;
			return cost;
		}
		else{//OR
			return childMaxCost;
		}
	}
}

class CostTuple extends Tuple3D<DataTypeEnum, OperatorTypeEnum, Double>{
	private static final long serialVersionUID = -5084368563329085326L;

	public CostTuple(DataTypeEnum first, OperatorTypeEnum second, Double third) {
		super(first, second, third);		
	}
}