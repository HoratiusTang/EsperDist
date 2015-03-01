package dist.esper.core.cost;

import java.util.List;

import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.epl.expr.Value;
import dist.esper.event.EventProperty;
import dist.esper.util.Tuple2D;
import dist.esper.util.Tuple4D;

public class SampleNumberFilterSimulator {
	static IntegerFilterSimulator nfs=new IntegerFilterSimulator();
	static FloatFilterSimulator ffs=new FloatFilterSimulator();
	static StringFilterSimulator sfs=new StringFilterSimulator();
	
	public static FilterSimulator getFilterSimlulator(EventProperty prop){
		if(prop.isInteger()){
			return nfs;
		}
		else if(prop.isFloat()){
			return ffs;
		}
		else if(prop.isString()){
			return sfs;
		}
		return null;
	}
	public static abstract class FilterSimulator extends Tuple4D<List<?>, Number, Number, Value>{//list,min,max,value
		OperatorTypeEnum op;
		public void setOperator(OperatorTypeEnum op) {
			this.op = op;
		}		
		public abstract double estimateSelectFactor();
	}
	
	static class IntegerFilterSimulator extends FilterSimulator{
		@Override
		public double estimateSelectFactor() {
			if(first.size()<=0){
				return Double.NEGATIVE_INFINITY;
			}			
			int nullCount=0; int lessCount=0; int equalCount=0; int greaterCount=0;			
			for(Object obj: this.first){
				Number t=(Number)obj;
				if(t==null){continue;}
				long comp=t.longValue()-fourth.getIntVal();
				if(comp==0L){equalCount++;}
				else if(comp<0L){lessCount++;}
				else{greaterCount++;}
			}
			if(op==OperatorTypeEnum.EQUAL){
				return (double)equalCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.LESS){
				return (double)lessCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL){
				return (double)(lessCount+equalCount)/(double)first.size();
			}
			else if(op==OperatorTypeEnum.GREATER){
				return (double)greaterCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.GREATER_OR_EQUAL){
				return (double)(greaterCount+equalCount)/(double)first.size();
			}
			return Double.NEGATIVE_INFINITY;
		}
	}
	
	static class FloatFilterSimulator extends FilterSimulator{
		@Override
		public double estimateSelectFactor() {
			if(first.size()<=0){
				return Double.NEGATIVE_INFINITY;
			}
			//double range=((Number)second).doubleValue()-((Number)third).doubleValue();
			int nullCount=0; int lessCount=0; int equalCount=0; int greaterCount=0;
			for(Object obj: this.first){
				Number t=(Number)obj;
				if(t==null){continue;}
				double comp=t.doubleValue()-fourth.getFloatVal();
				if(comp==0.0d){equalCount++;}
				else if(comp<0.0d){lessCount++;}
				else{greaterCount++;}
			}
			if(op==OperatorTypeEnum.EQUAL){
				return (double)equalCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.LESS){
				return (double)lessCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL){
				return (double)(lessCount+equalCount)/(double)first.size();
			}
			else if(op==OperatorTypeEnum.GREATER){
				return (double)greaterCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.GREATER_OR_EQUAL){
				return (double)(greaterCount+equalCount)/(double)first.size();
			}
			return Double.NEGATIVE_INFINITY;
		}
	}
	
	static class StringFilterSimulator extends FilterSimulator{
		@Override
		public double estimateSelectFactor() {
			if(first.size()<=0){
				return Double.NEGATIVE_INFINITY;
			}
			int nullCount=0; int lessCount=0; int equalCount=0; int greaterCount=0;			
			for(Object obj: this.first){
				String t=(String)obj;
				if(t==null){continue;}
				int comp=t.compareTo(fourth.getStrVal());
				if(comp==0L){equalCount++;}
				else if(comp<0L){lessCount++;}
				else{greaterCount++;}
			}
			if(op==OperatorTypeEnum.EQUAL){
				return (double)equalCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.LESS){
				return (double)lessCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL){
				return (double)(lessCount+equalCount)/(double)first.size();
			}
			else if(op==OperatorTypeEnum.GREATER){
				return (double)greaterCount/(double)first.size();
			}
			else if(op==OperatorTypeEnum.GREATER_OR_EQUAL){
				return (double)(greaterCount+equalCount)/(double)first.size();
			}
			return Double.NEGATIVE_INFINITY;
		}	
	}
}
