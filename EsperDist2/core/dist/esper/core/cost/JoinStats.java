package dist.esper.core.cost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dist.esper.core.cost.SampleNumberJoinSimulator.JoinSimulator;
import dist.esper.core.flow.container.*;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.ComparisonExpression;
import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.event.EventProperty;
import dist.esper.util.Tuple2D;

public class JoinStats {
	FilterStats filterStats;
	RawStats rawStats;
	Map<PropertyComparisonPair, JoinComparison> jcsMap=new HashMap<PropertyComparisonPair, JoinComparison>();
	
	public JoinStats(RawStats rawStats, FilterStats filterStats) {
		super();
		this.rawStats = rawStats;
		this.filterStats = filterStats;
	}

	public void registContainer(JoinStreamContainer jsc){
		if(jsc.getJoinExprList().size()!=1 || !(jsc.getJoinExprList().get(0) instanceof ComparisonExpression)){
			return;
		}
		ComparisonExpression ce=(ComparisonExpression)jsc.getJoinExprList().get(0);
		EventProperty prop1=((EventPropertySpecification)(ce.getChild(0))).getEventProp();
		EventProperty prop2=((EventPropertySpecification)(ce.getChild(1))).getEventProp();
		PropertyComparisonPair pcp=PropertyComparisonPair.makeFormalized(prop1, prop2, ce.getRelation());
		JoinComparison jcmp=jcsMap.get(pcp);
		if(jcmp==null){
			jcmp=new JoinComparison(pcp);
			jcsMap.put(pcp, jcmp);
		}
		JoinContainerComparison jcc=jcmp.jccMap.get(jsc.getUniqueName());
		if(jcc==null){
			jcc=new JoinContainerComparison(jsc.getUniqueName());
			jcmp.jccMap.put(jsc.getUniqueName(), jcc);
		}
	}
	
	public void updateContainerStat(JoinStreamContainer jsc, InstanceStat ss){
		if(jsc.getJoinExprList().size()!=1 || !(jsc.getJoinExprList().get(0) instanceof ComparisonExpression)){
			return;
		}
		ComparisonExpression ce=(ComparisonExpression)jsc.getJoinExprList().get(0);
		EventProperty prop1=((EventPropertySpecification)(ce.getChild(0))).getEventProp();
		EventProperty prop2=((EventPropertySpecification)(ce.getChild(1))).getEventProp();
		PropertyComparisonPair pcp=PropertyComparisonPair.makeFormalized(prop1, prop2, ce.getRelation());
		JoinComparison jcs=jcsMap.get(pcp);
		assert(jcs!=null);
		JoinContainerComparison jcc=jcs.jccMap.get(ss.uniqueName);
		assert(jcc!=null);
		jcc.inputCount[0] = ss.subStats[0].eventCount;
		jcc.inputCount[1] = ss.subStats[1].eventCount;
		jcc.outputCount = ss.eventCount;
		jcs.jccMap.put(ss.uniqueName, jcc);
		jcsMap.put(pcp, jcs);		
	}
	
	public double estimateSelectFactor(EventPropertySpecification eps1, EventPropertySpecification eps2, OperatorTypeEnum op){
		return estimateSelectFactor(eps1.getEventProp(), eps2.getEventProp(), op);
	}
	
	public double estimateSelectFactor(EventProperty prop1, EventProperty prop2, OperatorTypeEnum op){
		double sf=0.0d;
		PropertyComparisonPair pcp=PropertyComparisonPair.makeFormalized(prop1, prop2, op);
		JoinComparison jcs=jcsMap.get(pcp);
		if(jcs!=null){
			sf=jcs.estimateSelectFactor();
			return pcp.isOpposite ? sf : (1-sf);
		}
		else{
			sf=estimateSelectFactorBySimilarOperator(prop1, prop2, op);
			if(sf>=0.0d){
				return sf;
			}
			else{
				sf=estimateSelectFactorBySampleValues(prop1, prop2, op);
				if(sf>=0.0d){
					return sf;
				}
				else{
					return JoinComparison.DEFUALT_SELECT_FACTOR;
				}
			}	
		}
	}
	
	public double estimateSelectFactorBySimilarOperator(EventProperty prop1, EventProperty prop2, OperatorTypeEnum op){
		OperatorTypeEnum op2=null;
		if(op==OperatorTypeEnum.LESS) op2=OperatorTypeEnum.LESS_OR_EQUAL;
		else if(op==OperatorTypeEnum.LESS_OR_EQUAL) op2=OperatorTypeEnum.LESS;
		else if(op==OperatorTypeEnum.GREATER) op2=OperatorTypeEnum.GREATER_OR_EQUAL;
		else if(op==OperatorTypeEnum.GREATER_OR_EQUAL) op2=OperatorTypeEnum.GREATER;
		if(op2!=null){
			PropertyComparisonPair pcp=PropertyComparisonPair.makeFormalized(prop1, prop2, op2);
			JoinComparison jcs=jcsMap.get(pcp);
			if(jcs!=null){
				double sf=jcs.estimateSelectFactor();
				return pcp.isOpposite ? sf : (1-sf);
			}
		}
		return Double.NEGATIVE_INFINITY;
	}
	public double estimateSelectFactorBySampleValues(EventProperty prop1, EventProperty prop2, OperatorTypeEnum op){
		List<?> list1=rawStats.getSampleValues(prop1);
		List<?> list2=rawStats.getSampleValues(prop2);
		JoinSimulator js=SampleNumberJoinSimulator.getJoinSimulator(prop1, prop2, op);
		js.setFirst(list1);
		js.setSecond(list2);
		double sf=JoinComparison.DEFUALT_SELECT_FACTOR;
		try{
			sf=js.estimateSelectFactor();
		}
		catch(Exception ex){
			ex.printStackTrace();			
		}
		return sf;
	}
	
	/**
	public double estimateSelectFactorByRawStreamStats(EventProperty prop1, EventProperty prop2, OperatorTypeEnum op){
		NumberRange r1=rawStats.getPropertyRange(prop1);
		NumberRange r2=rawStats.getPropertyRange(prop2);
		if(!r1.isValid() || !r2.isValid()){
			return JoinComparison.DEFUALT_SELECT_FACTOR;
		}
		//sort, let r1.min < r2.min
		if(r2.min()<r1.min()){ NumberRange temp=r1; r1=r2; r2=temp;	op=op.reverse();}
		if(prop1.isInteger() && prop2.isInteger()){
			if(op==OperatorTypeEnum.EQUAL){
				if(r1.max()<r2.min()){ return JoinComparison.DEFUALT_SELECT_FACTOR/10;}
				else{ return (r1.max()-r2.min()+1.0d)/((r1.range()+1.0d)*(r2.range()+1.0d)); }
			}
			else{
				return JoinComparison.DEFUALT_SELECT_FACTOR;
			}
			
			else if(op==OperatorTypeEnum.LESS || op==OperatorTypeEnum.LESS_OR_EQUAL){
				double t1=r2.min()-r1.min();
				double t2=r1.max()-r2.min();
				return ((t1+1.0d)*(r2.range()+1.0d) + (t2+1.0d)*Math.max(r2.max()-r1.max(), 1.0d))/((r1.range()+1.0d)*(r2.range()+1.0d)); 
			}
		}
		if(prop1.isFloat() && prop2.isFloat()){
			if(op==OperatorTypeEnum.EQUAL){
				if(r2.min()<r1.min()){ NumberRange temp=r1; r1=r2; r2=temp;	}
				if(r1.max()<r2.min()){ return JoinComparison.DEFUALT_SELECT_FACTOR/10; }
				else{ return (r1.max()-r2.min())/(r1.range()*r2.range()+Double.MIN_NORMAL);	}
			}
		}		
		return 0.0d;
	}
	**/	
	public static class JoinComparison{
		public static double DEFUALT_SELECT_FACTOR=0.05d;
		PropertyComparisonPair propCompPair;
		Map<String, JoinContainerComparison> jccMap=new HashMap<String, JoinContainerComparison>();
		
		public JoinComparison(PropertyComparisonPair propPair) {
			super();
			this.propCompPair = propPair;
		}

		public double estimateSelectFactor(){
			long totalInput0=0;
			long totalInput1=0;
			long totalOutput=0;
			for(Map.Entry<String, JoinContainerComparison> e: jccMap.entrySet()){
				JoinContainerComparison jcc=e.getValue();
				totalInput0 += jcc.inputCount[0];
				totalInput1 += jcc.inputCount[1];
				totalOutput += jcc.outputCount;
			}
			if(totalInput0*totalInput1 <= 0 ){
				return DEFUALT_SELECT_FACTOR;
			}
			else{
				return (double)totalOutput/((double)totalInput0*(double)totalInput1);
			}
		}
	}
	
	public static class JoinContainerComparison{
		String id;
		long[] inputCount;
		long outputCount;
		public JoinContainerComparison(String id) {
			super();
			this.id = id;
			this.inputCount = new long[2];
			this.inputCount[0] = 0L;
			this.inputCount[1] = 0L;
			this.outputCount = 0;
		}
		
		public double computeSelectFactor(){
			if(inputCount[0]*inputCount[1] <= 0 ){
				return JoinComparison.DEFUALT_SELECT_FACTOR;
			}
			return (double)outputCount/((double)inputCount[0]*(double)inputCount[1]);
		}
	}
	
	public static class PropertyComparisonPair extends Tuple2D<EventProperty,EventProperty>{
		OperatorTypeEnum op;
		boolean isOpposite;
		
		public static PropertyComparisonPair makeFormalized(EventProperty left, EventProperty right, OperatorTypeEnum operator) {
			boolean isOpposite=false;
			if(left.fullName().compareTo(right.fullName())>0){
				EventProperty temp=left;
				left=right;
				right=temp;
				operator=operator.reverse();
			}
			if(operator==OperatorTypeEnum.GREATER || operator==OperatorTypeEnum.GREATER_OR_EQUAL){
				operator=operator.opposite();
				isOpposite=true;
			}
			return new PropertyComparisonPair(left, right, operator, isOpposite);
		}
		private PropertyComparisonPair(EventProperty left, EventProperty right, OperatorTypeEnum operator, boolean isOpposite){
			super(left, right);
			this.op=operator;
			this.isOpposite=isOpposite;
		}
	}
}
