package dist.esper.experiment2;

import java.util.ArrayList;
import java.util.List;

public class NodeList2{
	int numWay=0;
	List<NodeList> nodeListList=new ArrayList<NodeList>();
	
	public NodeList2(int numWay) {
		super();
		this.numWay = numWay;
	}

	public void addNodeList(NodeList nodeList){
		int row=nodeListList.size();			
		for(Node node: nodeList.nodeList){
			node.setNumWay(numWay);
			node.setRow(row);
		}
		nodeListList.add(nodeList);
	}
	
	public int getNodeListsCount(){
		return nodeListList.size();
	}
	
//	public int getNodesTotalCount(){
//		int count=0;
//		for(NodeList nodeList: nodeListList){
//			count+=nodeList.getNodesCount();
//		}
//		return count;
//	}
	
	public void resetPop(){
		for(NodeList nodeList: nodeListList){
			nodeList.resetPop();
		}
	}
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("=================== NodeList2 (%d) ====================\n", numWay));
		for(NodeList n: nodeListList){
			sb.append(n.toString());
			sb.append("\n");
		}
		return sb.toString();
	}
}
