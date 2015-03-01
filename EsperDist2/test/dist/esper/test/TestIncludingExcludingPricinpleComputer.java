package dist.esper.test;

import dist.esper.util.IncludingExcludingPrincipleComputer;

public class TestIncludingExcludingPricinpleComputer {
	public static void main(String[] args){
		test1();
	}
	
	public static void test1(){
		double[] vals={0.9,0.7};
		double result=IncludingExcludingPrincipleComputer.compute(vals);
		
		System.out.println(result);
	}
}
