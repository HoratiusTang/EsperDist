package dist.esper.core.cost;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.epl.expr.Value;
import dist.esper.util.Tuple2D;

public abstract class AbstractPropertyStat<T> implements Serializable{	
	private static final long serialVersionUID = 8601186477544588736L;
	public transient static int DEFUALT_SAMPLE_SIZE=10;
	public transient static double DEFAULT_PROBABILITY=0.05d;
	public String propName;
	public int currentSize=0;
	
	public AbstractPropertyStat() {
		super();
	}
	
	public AbstractPropertyStat(String propName) {
		super();
		this.propName = propName;
	}

	public String getPropName() {
		return propName;
	}

	public void setPropName(String propName) {
		this.propName = propName;
	}

	public int getCurrentSize() {
		return currentSize;
	}

	public void setCurrentSize(int currentSize) {
		this.currentSize = currentSize;
	}

	public abstract void update(Object val);
	
	public abstract void toStringBuilder(StringBuilder sb);
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		this.toStringBuilder(sb);
		return sb.toString();
	}
	
	public abstract static class ValuePropertyStat<T extends Comparable<T>> extends AbstractPropertyStat<T>{
		private static final long serialVersionUID = -873318133265885878L;
		public T min;
		public T max;
		public T[] sampleValues;
		public Class<T> type;
		
		public Class<T> getType() {
			return type;
		}
		public void setType(Class<T> type) {
			this.type = type;
		}
		public void setMin(T min) {
			this.min = min;
		}
		public void setMax(T max) {
			this.max = max;
		}
		public void setSampleValues(T[] sampleValues) {
			this.sampleValues = sampleValues;
		}
		public ValuePropertyStat(){
			super();
		}
		public ValuePropertyStat(String propName, Class<T> type) {
			super(propName);
			this.type=type;			
			sampleValues=(T[])Array.newInstance(type, DEFUALT_SAMPLE_SIZE);
			Arrays.fill(sampleValues, null);
		}
		public List<T> getSampleValues() {
			if(currentSize < sampleValues.length){
				List<T> list=new ArrayList<T>(currentSize);
				for(int i=0;i<currentSize;i++){
					list.add(sampleValues[i]);
				}
				return list;
			}
			return Arrays.asList(sampleValues);
		}
		public T getMin() {
			return min;
		}

		public T getMax() {
			return max;
		}

		@Override
		public void update(Object val) {
			T t=(T)val;
			if(currentSize < sampleValues.length){
				try{
					sampleValues[currentSize]=t;
				}
				catch(Exception ex){
					ex.printStackTrace();
				}
				currentSize++;
			}
			else{
				int index=(int)(Math.random()*(double)sampleValues.length);				
				sampleValues[index]=t;
			}
			if(min==null || t.compareTo(min)<0){
				min=t;
			}
			if(max==null || t.compareTo(max)>0){
				max=t;
			}
		}

		@Override
		public void toStringBuilder(StringBuilder sb) {
			String dem="";
			sb.append(this.propName);
			sb.append(":");
			sb.append(this.getClass().getSimpleName());
			sb.append("<"+type.getSimpleName()+"> [");
			sb.append(min);	sb.append(",");	sb.append(max);	sb.append("][");
			for(int i=0;i<sampleValues.length;i++){
				sb.append(dem);
				sb.append(sampleValues[i]==null?"null":sampleValues[i].toString());
				dem=", ";
			}
			sb.append(']');
		}
	}
	
	public abstract static class NumberPropertyStat<T extends Comparable<T>> extends ValuePropertyStat<T>{		
		private static final long serialVersionUID = 6020936031032100482L;

		public NumberPropertyStat(){
			super();
		}
		public NumberPropertyStat(String propName, Class<T> type) {
			super(propName, type);			
		}
	}
	
	public static class IntegerPropertyStat<T extends Comparable<T>> extends NumberPropertyStat<T>{
		private static final long serialVersionUID = -8835317599060384465L;

		public IntegerPropertyStat(){
			super();
		}
		public IntegerPropertyStat(String propName, Class<T> type) {
			super(propName, type);
		}
	}
	
	public static class FloatPropertyStat<T extends Comparable<T>> extends NumberPropertyStat<T>{
		private static final long serialVersionUID = 4416237375003674708L;

		public FloatPropertyStat(){
			super();
		}
		public FloatPropertyStat(String propName, Class<T> type) {
			super(propName, type);
		}		
	}
	
	public static class ArrayPropertyStat extends AbstractPropertyStat<Object>{		
		private static final long serialVersionUID = -3768011821727117501L;
		public int minLength=0;
		public int maxLength=0;		
		public int[] sampleLengths;
		
		public ArrayPropertyStat() {
			super();
		}

		public int getMinLength() {
			return minLength;
		}

		public void setMinLength(int minLength) {
			this.minLength = minLength;
		}

		public int getMaxLength() {
			return maxLength;
		}

		public void setMaxLength(int maxLength) {
			this.maxLength = maxLength;
		}

		public int[] getSampleLengths() {
			return sampleLengths;
		}

		public void setSampleLengths(int[] sampleLengths) {
			this.sampleLengths = sampleLengths;
		}

		public ArrayPropertyStat(String propName) {
			super(propName);
			sampleLengths=new int[DEFUALT_SAMPLE_SIZE];
			Arrays.fill(sampleLengths, -1);
		}
		
		public double getAvgLength(){
			int totalLength=0;
			int count=0;
			for(int length: sampleLengths){
				if(length>=0){
					totalLength += length;
					count++;
				}
			}
			if(count>0){
				return (double)totalLength/(double)count;
			}
			return 0.0;
		}

		@Override
		public void update(Object val) {
			int length=val==null?0:Array.getLength(val);
			if(currentSize < sampleLengths.length){
				sampleLengths[currentSize]=length;
				currentSize++;
			}
			else{
				int index=(int)(Math.random()*(double)sampleLengths.length);
				sampleLengths[index]=length;
			}
		}

		@Override
		public void toStringBuilder(StringBuilder sb) {
			String dem="";
			sb.append(this.propName);
			sb.append(':');
			sb.append(this.getClass().getSimpleName());
			sb.append('[');
			for(int i=0;i<sampleLengths.length;i++){
				sb.append(dem);
				sb.append(sampleLengths[i]);
				dem=", ";
			}
			sb.append(']');
		}
	}
	
	public static class StringPropertyStat extends ValuePropertyStat<String>{
		private static final long serialVersionUID = 5982439455409820789L;
		public StringPropertyStat(){
			super();
		}
		public StringPropertyStat(String propName) {
			super(propName, String.class);
		}
		public double getAvgLength(){
			int totalLength=0;
			for(String str: this.sampleValues){
				totalLength += (str==null)?0:str.length();
			}
			return (double)totalLength/this.sampleValues.length;
		}		
	}
}

