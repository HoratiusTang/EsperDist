package dist.esper.external.event;

import java.util.Random;

public class FieldGeneratorFactory {
	public static class LongMonotoGenerator extends FieldGenerator{
		long n=0;
		long step=1;
		
		public LongMonotoGenerator(long n0, long step){
			this.n=n0;
			this.step=step;
		}
		@Override
		protected long nextLong(){
			n += step;
			return n;
		}
		@Override
		public Object next() {			
			return Long.valueOf(nextLong());
		}
	}
	
	public static class IntegerMonotoGenerator extends LongMonotoGenerator{
		public IntegerMonotoGenerator(int n0, int step) {
			super(n0, step);
		}

		@Override
		public Object next() {
			return Integer.valueOf((int)nextLong());
		}
	}
	
	public static class DoubleMonotoGenerator extends FieldGenerator{
		double n=0.0d;
		double step=1.0d;
		
		public DoubleMonotoGenerator(double n0, double step){
			this.n=n0;
			this.step=step;
		}
		
		protected double nextDouble(){
			n += step;
			return n;
		}
		@Override
		public Object next() {			
			return Double.valueOf(nextDouble());
		}
	}
	
	public static class LongUniformGenerator extends FieldGenerator{
		long min;
		long max;
		Random rand;
		
		public LongUniformGenerator(long min, long max) {
			super();
			this.min = min;
			this.max = max;
			rand = new Random();
		}
		
		@Override
		protected long nextLong(){
			long n=min + rand.nextInt((int)(max-min));
			return n;
		}

		@Override
		public Object next() {			
			return Long.valueOf(nextLong());
		}
	}
	
	public static class IntegerUniformGenerator extends LongUniformGenerator{
		public IntegerUniformGenerator(int min, int max) {
			super(min, max);			
		}
		@Override
		public Object next() {			
			return Integer.valueOf((int)nextLong());
		}
	}
	
	public static class DoubleUniformGenerator extends FieldGenerator{
		double min;
		double max;
		Random rand;
		
		public DoubleUniformGenerator(double min, double max) {
			super();
			this.min = min;
			this.max = max;
			rand = new Random();
		}
		
		@Override
		protected double nextDouble(){
			double n=min+rand.nextDouble()*(max-min);
			return n;
		}
		
		@Override
		public Object next() {
			return Double.valueOf(nextDouble());
		}		
	}
	
	public static class DoubleNormalGenerator extends FieldGenerator{
		double standardDeviation=1.0d;
		double expectedValue=0;
		double minValue=-Double.MAX_VALUE;
		double maxValue=Double.MAX_VALUE;
		Random rand;
		
		public DoubleNormalGenerator(
				double expectedValue,
				double standardDeviation){
			this(expectedValue, standardDeviation, -Double.MAX_VALUE, Double.MAX_VALUE);
		}
		
		public DoubleNormalGenerator(
				double expectedValue,
				double standardDeviation,
				double minValue,
				double maxValue) {
			super();
			this.expectedValue = expectedValue;
			this.standardDeviation = standardDeviation;
			this.minValue = minValue;
			this.maxValue = maxValue;
			this.rand = new Random();
		}
		
		@Override
		public double nextDouble(){
			double g=rand.nextGaussian();
			double v=g*standardDeviation+expectedValue;
			if(v<minValue){
				return minValue;
			}
			if(v>maxValue){
				return maxValue;
			}
			return v;
		}

		@Override
		public Object next() {
			return Double.valueOf(nextDouble());
		}
	}
	
	public static class IntegerNormalGenerator extends DoubleNormalGenerator{
		public IntegerNormalGenerator(double expectedValue,
				double standardDeviation) {
			super(expectedValue, standardDeviation, Integer.MIN_VALUE, Integer.MAX_VALUE);
		}
		

		public IntegerNormalGenerator(double expectedValue,
				double standardDeviation, double minValue, double maxValue) {
			super(expectedValue, standardDeviation, minValue, maxValue);
		}


		@Override
		public Object next() {
			return Integer.valueOf((int)nextDouble());
		}
	}
	
	public static class LongNormalGenerator extends DoubleNormalGenerator{
		public LongNormalGenerator(double expectedValue,
				double standardDeviation) {
			super(expectedValue, standardDeviation, Long.MIN_VALUE, Long.MAX_VALUE);
		}

		public LongNormalGenerator(double expectedValue,
				double standardDeviation, double minValue, double maxValue) {
			super(expectedValue, standardDeviation, minValue, maxValue);
		}


		@Override
		public Object next() {
			return Long.valueOf((long)nextDouble());
		}
	}
	
	public static class IntArrayGenerator extends FieldGenerator{
		int[] array;
		IntegerUniformGenerator intGen;
		
		public IntArrayGenerator(int size) {
			super();
			array=new int[size];
			intGen=new IntegerUniformGenerator(100, 1000);
		}

		@Override
		public Object next() {
			for(int i=0;i<array.length;i++){
				array[i]=(int)intGen.nextLong();
			}
			return array;
		}
	}
	
	public static class IntegerArrayGenerator extends FieldGenerator{
		Integer[] array;
		IntegerUniformGenerator intGen;
		
		private IntegerArrayGenerator(int size) {
			super();
			array=new Integer[size];
			intGen=new IntegerUniformGenerator(100, 1000);
		}

		@Override
		public Object next() {
			for(int i=0;i<array.length;i++){
				array[i]=(int)intGen.nextLong();
			}
			return array;
		}
	}
	
	public static class Long0ArrayGenerator extends FieldGenerator{
		long[] array;
		LongUniformGenerator longGen;
		
		private Long0ArrayGenerator(int size) {
			super();
			array=new long[size];
			longGen=new LongUniformGenerator(100, 1000);
		}

		@Override
		public Object next() {
			for(int i=0;i<array.length;i++){
				array[i]=longGen.nextLong();
			}
			return array;
		}
	}
	
	public static class Long1ArrayGenerator extends FieldGenerator{
		Long[] array;
		LongUniformGenerator longGen;
		
		private Long1ArrayGenerator(int size) {
			super();
			array=new Long[size];
			longGen=new LongUniformGenerator(100, 1000);
		}

		@Override
		public Object next() {
			for(int i=0;i<array.length;i++){
				array[i]=longGen.nextLong();
			}
			return array;
		}
	}
	
	public static class Double0ArrayGenerator extends FieldGenerator{
		double[] array;
		DoubleUniformGenerator doubleGen;
		
		private Double0ArrayGenerator(int size) {
			super();
			array=new double[size];
			doubleGen=new DoubleUniformGenerator(100, 1000);
		}

		@Override
		public Object next() {
			for(int i=0;i<array.length;i++){
				array[i]=doubleGen.nextDouble();
			}
			return array;
		}
	}
	
	public static class Double1ArrayGenerator extends FieldGenerator{
		Double[] array;
		DoubleUniformGenerator doubleGen;
		
		private Double1ArrayGenerator(int size) {
			super();
			array=new Double[size];
			doubleGen=new DoubleUniformGenerator(100, 1000);
		}

		@Override
		public Object next() {
			for(int i=0;i<array.length;i++){
				array[i]=doubleGen.nextDouble();
			}
			return array;
		}
	}
	
	public static class StringRandomChooser extends FieldGenerator{
		String[] strs;
		Random rand;
		public StringRandomChooser(String[] strs){
			this.strs = strs;
			rand=new Random();
		}
		
		@Override
		public Object next() {
			int index=rand.nextInt(strs.length);
			return strs[index];
		}		
	}
}
