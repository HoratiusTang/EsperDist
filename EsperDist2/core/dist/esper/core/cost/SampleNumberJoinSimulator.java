package dist.esper.core.cost;

import java.util.List;

import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.event.EventProperty;
import dist.esper.util.Tuple2D;

public class SampleNumberJoinSimulator {
	static JoinSimulator joinSimulator=new JoinSimulator();
	static IntegerEqualComparator neqComparator=new IntegerEqualComparator();
	static IntegerLessComparator nltComparator=new IntegerLessComparator();
	static IntegerLessOrEqualComparator nleComparator=new IntegerLessOrEqualComparator();
	static IntegerGreaterComparator ngtComparator=new IntegerGreaterComparator();
	static IntegerGreaterOrEqualComparator ngeComparator=new IntegerGreaterOrEqualComparator();
	
	static FloatEqualComparator feqComparator=new FloatEqualComparator();
	static FloatLessComparator fltComparator=new FloatLessComparator();
	static FloatLessOrEqualComparator fleComparator=new FloatLessOrEqualComparator();
	static FloatGreaterComparator fgtComparator=new FloatGreaterComparator();
	static FloatGreaterOrEqualComparator fgeComparator=new FloatGreaterOrEqualComparator();
	
	static StringEqualComparator seqComparator = new StringEqualComparator();
	static StringLessComparator sltComparator = new StringLessComparator();
	static StringLessOrEqualComparator sleComparator = new StringLessOrEqualComparator();
	static StringGreaterComparator sgtComparator = new StringGreaterComparator();
	static StringGreaterOrEqualComparator sgeComparator = new StringGreaterOrEqualComparator();
	
	public static JoinSimulator getJoinSimulator(EventProperty prop1, EventProperty prop2, OperatorTypeEnum op){
		Comparator<?> comparator=null;
		if(prop1.isFloat() || prop2.isFloat()){
			if(op==OperatorTypeEnum.EQUAL) comparator=neqComparator;
			else if(op==OperatorTypeEnum.LESS) comparator=nltComparator;
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL) comparator=nleComparator;
			else if(op==OperatorTypeEnum.GREATER) comparator=ngtComparator;
			else if(op==OperatorTypeEnum.GREATER_OR_EQUAL) comparator=ngeComparator;
		}
		else if(prop1.isString()){
			if(op==OperatorTypeEnum.EQUAL) comparator=seqComparator;
			else if(op==OperatorTypeEnum.LESS) comparator=sltComparator;
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL) comparator=sleComparator;
			else if(op==OperatorTypeEnum.GREATER) comparator=sgtComparator;
			else if(op==OperatorTypeEnum.GREATER_OR_EQUAL) comparator=sgeComparator;
		}
		else{
			if(op==OperatorTypeEnum.EQUAL) comparator=feqComparator;
			else if(op==OperatorTypeEnum.LESS) comparator=fltComparator;
			else if(op==OperatorTypeEnum.LESS_OR_EQUAL) comparator=fleComparator;
			else if(op==OperatorTypeEnum.GREATER) comparator=fgtComparator;
			else if(op==OperatorTypeEnum.GREATER_OR_EQUAL) comparator=fgeComparator;
		}
		joinSimulator.setComparator(comparator);
		return joinSimulator;
	}
	
	static abstract class Comparator<T>{
		public abstract boolean compare(T a, T b);
	}
	
	static abstract class NumberComparator extends Comparator<Number>{	
	}
	
	static abstract class StringComparator extends Comparator<String>{		
	}
	
	static class IntegerEqualComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.longValue()==b.longValue();
		}
	}
	
	static class IntegerLessComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.longValue()<b.longValue();
		}
	}
	
	static class IntegerLessOrEqualComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.longValue()<=b.longValue();
		}
	}
	
	static class IntegerGreaterComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.longValue()>b.longValue();
		}
	}
	
	static class IntegerGreaterOrEqualComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.longValue()>=b.longValue();
		}
	}
	
	static class FloatEqualComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.doubleValue()==b.doubleValue();
		}
	}
	
	static class FloatLessComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.doubleValue()<b.doubleValue();
		}
	}
	
	static class FloatLessOrEqualComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.doubleValue()<=b.doubleValue();
		}
	}
	
	static class FloatGreaterComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.doubleValue()>b.doubleValue();
		}
	}
	
	static class FloatGreaterOrEqualComparator extends NumberComparator{
		public boolean compare(Number a, Number b){
			return a.doubleValue()>=b.doubleValue();
		}
	}
	
	static class StringEqualComparator extends StringComparator{
		public boolean compare(String a, String b){
			return a.compareTo(b)==0;
		}
	}
	
	static class StringLessComparator extends StringComparator{
		public boolean compare(String a, String b){
			return a.compareTo(b)<0;
		}
	}
	
	static class StringLessOrEqualComparator extends StringComparator{
		public boolean compare(String a, String b){
			return a.compareTo(b)<=0;
		}
	}
	
	static class StringGreaterComparator extends StringComparator{
		public boolean compare(String a, String b){
			return a.compareTo(b)>0;
		}
	}
	
	static class StringGreaterOrEqualComparator extends StringComparator{
		public boolean compare(String a, String b){
			return a.compareTo(b)>=0;
		}
	}
	
	public static class JoinSimulator extends Tuple2D<List<?>, List<?>>{
		Comparator<?> comparator=null;

		public Comparator<?> getComparator() {
			return comparator;
		}

		public void setComparator(Comparator<?> comparator) {
			this.comparator = comparator;
		}

		public double estimateSelectFactor(){
			if(first.size()==0 || second.size()==0){
				return Double.NEGATIVE_INFINITY;
			}
			int trueCount=0;
			
			if(comparator instanceof NumberComparator){
				NumberComparator numberComp=(NumberComparator)comparator;
				for(Object a: first){
					if(a==null){continue;}
					for(Object b: first){
						if(b==null){continue;}
						if(numberComp.compare((Number)a, (Number)b)){
							trueCount++;
						}
					}
				}
			}
			else{
				StringComparator strComp=(StringComparator)comparator;
				for(Object a: first){
					if(a==null){continue;}
					for(Object b: first){
						if(b==null){continue;}
						if(strComp.compare((String)a, (String)b)){
							trueCount++;
						}
					}
				}
			}
			return (double)trueCount/(double)(first.size()*second.size());
		}
	}
}
