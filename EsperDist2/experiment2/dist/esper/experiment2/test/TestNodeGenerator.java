package dist.esper.experiment2.test;

import dist.esper.experiment2.NodeList2;
import dist.esper.experiment2.NodesGenerator;
import dist.esper.experiment2.NodesParameter;

public class TestNodeGenerator {
	public static void main(String[] args){
		test1();
	}
	
	public static void test1(){
		int numEventTypes=10;
		int numPropTypes=6;
		int numNodePerType=20;
		
		NodesParameter[] nps=new NodesParameter[]{
			new NodesParameter(1, 10, 0.3, 0.3),
			new NodesParameter(2, 10, 0.3, 0.3),
			new NodesParameter(3, 10, 0.3, 0.3),
			new NodesParameter(4, 10, 0.3, 0.3),
			new NodesParameter(5, 10, 0.3, 0.3),
		};
		
		NodesGenerator ng=new NodesGenerator(numEventTypes, numPropTypes, numNodePerType, nps);
		NodeList2[] nl2s=ng.genearteNodeList2s();
		
		for(NodeList2 nl2: nl2s){
			if(nl2!=null){
				System.out.println(nl2);
			}
		}
	}
}
