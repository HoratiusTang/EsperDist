package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.external.event.*;

public class QueryGenerator2 {
	EventInstanceGenerator[] eigs;
	int numEventTypes;
	int numPropTypes;
	OperatorTypeEnum[] filterOpTypes;
	OperatorTypeEnum[] joinOpTypes;
	int[] windowTimes;
	NodeList2[] nodeList2s;
	
	public List<String> generateQueries(){
		return null;
	}
	
	public void generateNodeList2s(){
		
	}
	
	public void generateValues(){
		for(NodeList2 nl2: nodeList2s){
			for(NodeList nl: nl2.nodeListList){
				generateValues(nl);
			}
		}
	}
	
	public void generateValues(NodeList nl){
		if(nl.getTypeCount()==1){//filter
		
		}
		else{//join
			
		}
	}
}
