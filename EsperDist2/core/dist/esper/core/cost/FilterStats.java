package dist.esper.core.cost;

import java.util.*;

import dist.esper.core.flow.container.*;
import dist.esper.epl.expr.*;
import dist.esper.event.*;
import dist.esper.util.Logger2;

public class FilterStats {
	static Logger2 log=Logger2.getLogger(FilterStats.class);
	public static double DEFAULT_SELECT_FACTOR=0.2;
	RawStats rawStats;
	Map<String, PropertyValueIndex> pviMap=new HashMap<String, PropertyValueIndex>();//indexed by prop.fullName(), or uniqueName?
	
	public FilterStats(RawStats rawStats) {
		super();
		this.rawStats = rawStats;
	}
	
	public void registContainer(FilterDelayedStreamContainer fcsc){
//		if(fcsc.getExtraFilterCondList().size()!=1 || 
//				!(fcsc.getExtraFilterCondList().get(0) instanceof ComparisonExpression) ||
//				!(fcsc.getAgent().getFilterExpr() instanceof ComparisonExpression)){
//			return;
//		}
		return;
	}

	public void registContainer(FilterStreamContainer fsc){
		if(!(fsc.getFilterExpr() instanceof ComparisonExpression)){
			return;
		}
		ComparisonExpression ce=(ComparisonExpression)fsc.getFilterExpr();
		EventProperty prop=((EventPropertySpecification)(ce.getChild(0))).getEventProp();
		Value value=(Value)ce.getChild(1);
		PropertyValueIndex pvi=pviMap.get(prop.fullName());
		if(pvi==null){
			pvi=new PropertyValueIndex(prop, value.type);
			pviMap.put(prop.fullName(), pvi);
		}
		
		ValueContainerRate vcr=new ValueContainerRate(fsc.getUniqueName(), prop, value, ce.getRelation());
//		if(fsc instanceof FilterCompatibleStreamLocationContainer){
//			FilterStreamLocationContainer agent=((FilterCompatibleStreamLocationContainer)fsc).getAgent();
//			ComparisonExpression agentCE=(ComparisonExpression)agent.getFilterExpr();
//			EventProperty agentCeProp=((EventPropertySpecification)(agentCE.getChild(0))).getEventProperty();
//			Value agentCeValue=(Value)agentCE.getChild(1);
//			ValueContainerRate agentVCR=locateValueContainerRate(agentCeProp, agentCeValue, ce.getRelation(), agent.getId());
//			assert(agentVCR!=null);
//			vcr.setAgentValueContainerRate(agentVCR);
//		}
		
		for(int i=0;i<pvi.valueRateList.size();i++){
			int compResult=Value.compareValue(value, pvi.valueRateList.get(i).value);
			if(compResult==0){
				pvi.valueRateList.get(i).addValueContainerRate(vcr);
				break;
			}
			else if(compResult<0){
				ValueRate vr=new ValueRate(prop, value);
				vr.addValueContainerRate(vcr);
				pvi.valueRateList.add(i, vr);
				break;
			}
		}		
	}
	
	//locate existing one
	public ValueContainerRate locateValueContainerRate(EventProperty prop, Value value, OperatorTypeEnum op, long containerId){
		PropertyValueIndex pvi=pviMap.get(prop.fullName());
		for(int i=0;i<pvi.valueRateList.size();i++){//at least two
			int compResult=Value.compareValue(value, pvi.valueRateList.get(i).value);
			if(compResult==0){
				ValueContainerRate vcr=pvi.valueRateList.get(i).getMapByOperator(op).get(containerId);
				return vcr;
			}
		}
		return null;
	}
	
	public void updateContainerStat(FilterStreamContainer fsc, InstanceStat ss){
		if(!(fsc.getFilterExpr() instanceof ComparisonExpression)){
			return;
		}
		ComparisonExpression ce=(ComparisonExpression)fsc.getFilterExpr();
		EventProperty prop=((EventPropertySpecification)(ce.getChild(0))).getEventProp();
		Value value=(Value)ce.getChild(1);
		PropertyValueIndex pvi=pviMap.get(prop.fullName());
		if(pvi==null){
			System.out.println("");
		}
		
		for(int i=0;i<pvi.valueRateList.size();i++){//at least two
			int compResult=Value.compareValue(value, pvi.valueRateList.get(i).value);
			if(compResult==0){
				ValueContainerRate vcr=pvi.valueRateList.get(i).getMapByOperator(ce.getRelation()).get(ss.uniqueName);
				vcr.durationUS=ss.durationUS();
				vcr.outputCount=ss.eventCount;
				vcr.inputCount=ss.subStats[0].eventCount;
				pvi.valueRateList.get(i).getMapByOperator(ce.getRelation()).put(ss.uniqueName, vcr);
				break;
			}
		}		
	}
	
	public double estimateAbsoluteSelectFactor(EventPropertySpecification eps, Value value, OperatorTypeEnum op){
		return estimateAbsoluteSelectFactor(eps.getEventProp(), value, op);
	}
	
	public double estimateAbsoluteSelectFactor(EventProperty prop, Value value, OperatorTypeEnum op){
		double sf=Double.NEGATIVE_INFINITY;
		PropertyValueIndex pvi=pviMap.get(prop.fullName());
		if(pvi!=null){
			int i=0;
			for(i=0;i<pvi.valueRateList.size();i++){//at least two
				ValueRate vr=pvi.valueRateList.get(i);
				if(Value.compareValue(value, vr.value)==0){
					sf=vr.estimateAbsoluteSelectFactor(op);
					break;
				}
			}
		}
		if(sf<0.0){
			sf=rawStats.estimateAbsoluteSelectFactor(prop, value, op);
		}
		return sf>=0.0d ? sf : DEFAULT_SELECT_FACTOR;
	}
	
	public class PropertyValueIndex{
		EventProperty prop;
		DataTypeEnum type;
		List<ValueRate> valueRateList=new ArrayList<ValueRate>();//list[0].value=min; list[end].value=max
		public PropertyValueIndex(EventProperty prop, DataTypeEnum type) {
			super();
			this.prop = prop;
			this.type = type;
			//TODO: add max one
		}
	}
	
	public class ValueRate{
		EventProperty prop;
		Value value;
		//double rate;		
		Map<String,ValueContainerRate> equalMap=new HashMap<String,ValueContainerRate>();
		Map<String,ValueContainerRate> lessMap=new HashMap<String,ValueContainerRate>();
		Map<String,ValueContainerRate> lessEqualMap=new HashMap<String,ValueContainerRate>();
		
		public Map<String,ValueContainerRate> getMapByOperator(OperatorTypeEnum op){
			if(op==OperatorTypeEnum.EQUAL){
				return equalMap;
			}
			else if(op==OperatorTypeEnum.LESS){
				return lessMap;
			}
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL){
				return lessEqualMap;
			}
			return null;
		}
		
		public ValueRate(EventProperty prop, Value value) {
			super();
			this.prop = prop;
			this.value = value;
		}
		
		public void addValueContainerRate(ValueContainerRate vcr){
			Map<String,ValueContainerRate> map=getMapByOperator(vcr.op);
			map.put(vcr.containerName, vcr);
		}

		public boolean hasEnoughData(){
			long maxOutputCount=0;
			long maxDuration=0;
			for(Map.Entry<String, ValueContainerRate> e: lessMap.entrySet()){
				if(maxOutputCount<e.getValue().outputCount){
					maxOutputCount=e.getValue().outputCount;
				}
				if(maxDuration<e.getValue().durationUS){
					maxDuration=e.getValue().durationUS;
				}
			}
			if(maxOutputCount>100 || 
					(maxOutputCount>20 && maxDuration>60)){//minutes
				return true;
			}
			return false;
		}
		
		public double estimateAbsoluteSelectFactor(OperatorTypeEnum op){
			if(this.getMapByOperator(op).size()>0){
				return computeAbsoluteSelectFactor(op);
			}
			if(op==OperatorTypeEnum.EQUAL){
				if(getMapByOperator(OperatorTypeEnum.LESS).size()>0 && 
					getMapByOperator(OperatorTypeEnum.LESS_OR_EQUAL).size()>0){
					double ltRate=computeAbsoluteSelectFactor(OperatorTypeEnum.LESS);
					double leRate=computeAbsoluteSelectFactor(OperatorTypeEnum.LESS_OR_EQUAL);
					double eqRate=leRate-ltRate;
					return eqRate>=0.0?eqRate:0.0;
				}
			}
			else if(op==OperatorTypeEnum.LESS){
				if(getMapByOperator(OperatorTypeEnum.LESS_OR_EQUAL).size()>0){
					double leRate=computeAbsoluteSelectFactor(OperatorTypeEnum.LESS_OR_EQUAL);
					double ltRate=leRate;
					double eqRate=Double.NEGATIVE_INFINITY;
					if(getMapByOperator(OperatorTypeEnum.EQUAL).size()>0){
						eqRate=computeAbsoluteSelectFactor(OperatorTypeEnum.EQUAL);
						ltRate-=eqRate;
					}
					return ltRate>0.0?ltRate:0.0;
				}
			}
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL){
				if(getMapByOperator(OperatorTypeEnum.LESS).size()>0){
					double ltRate=computeAbsoluteSelectFactor(OperatorTypeEnum.LESS);
					double leRate=ltRate;
					double eqRate=Double.NEGATIVE_INFINITY;
					if(getMapByOperator(OperatorTypeEnum.EQUAL).size()>0){
						eqRate=computeAbsoluteSelectFactor(OperatorTypeEnum.EQUAL);
						leRate+=eqRate;
					}
					return leRate>0.0?leRate:0.0;
				}
			}
			return Double.NEGATIVE_INFINITY;
		}
		
		public double computeAbsoluteSelectFactor(OperatorTypeEnum op){
			ValueContainerRate maxOutputVCR=null;
			for(ValueContainerRate e: getMapByOperator(op).values()){
				if(maxOutputVCR==null || maxOutputVCR.outputCount<e.outputCount){
					maxOutputVCR=e;
				}
			}
			if(maxOutputVCR!=null){
				return maxOutputVCR.computeAbsoluteSelectFactor();
			}
			return Double.NEGATIVE_INFINITY;
		}
	}
	
	public class ValueContainerRate{
		String containerName;
		OperatorTypeEnum op;
		EventProperty prop;
		Value value;
		ValueContainerRate agentVCR;
		long durationUS;
		long inputCount;
		long outputCount;
		public ValueContainerRate(String id, EventProperty prop, Value value, OperatorTypeEnum op) {
			super();
			this.containerName = id;
			this.prop = prop;
			this.value = value;
			this.op = op;
			this.agentVCR = null;
		}
		public ValueContainerRate getAgentValueContainerRate() {
			return agentVCR;
		}
		public void setAgentValueContainerRate(ValueContainerRate agentVCR) {
			this.agentVCR = agentVCR;
		}
		public double computeOutputRate(){
			return (double)outputCount/(double)durationUS;
		}
		public double computeRelativeSelectFactor(){
			return (double)outputCount/(double)inputCount;
		}
		public double computeAbsoluteSelectFactor(){
			double sf=this.computeRelativeSelectFactor();
			if(agentVCR!=null){
				sf*=agentVCR.computeAbsoluteSelectFactor();
			}
			return sf;
		}
	}
}
