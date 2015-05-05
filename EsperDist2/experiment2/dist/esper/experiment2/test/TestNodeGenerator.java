package dist.esper.experiment2.test;

import dist.esper.experiment2.NodesGenerator;
import dist.esper.experiment2.data.NodeListContainer;
import dist.esper.experiment2.data.NodesParameter;

public class TestNodeGenerator {
	public static void main(String[] args){
		test1();
	}
	
	public static void test1(){
		int numEventTypes=10;
		int numPropTypes=6;
		int numFilterOpTypes=2;
		int numJoinOpTypes=3;
		int numWindowTypes=3;
		int numSelectElementsPerFilter=3;
		
		NodesParameter[] nps=new NodesParameter[]{
			new NodesParameter(1, 60, 15, 0.3, 0.2),
			new NodesParameter(2, 36, 12, 0.3, 0.3),
			new NodesParameter(3, 24, 8, 0.3, 0.3),
			new NodesParameter(4, 10, 5, 0.3, 0.3),
			new NodesParameter(5, 10, 5, 0.3, 0.3),
		};
		
		NodesGenerator ng=new NodesGenerator(numEventTypes, numPropTypes, 
				numFilterOpTypes, numJoinOpTypes, numWindowTypes, 
				numSelectElementsPerFilter, nps);
		NodeListContainer[] nl2s;
		try {
			nl2s = ng.genearteNodeListContainers();
			for(NodeListContainer nl2: nl2s){
				if(nl2!=null){
					System.out.println(nl2);
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
