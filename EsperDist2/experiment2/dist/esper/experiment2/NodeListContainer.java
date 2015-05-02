package dist.esper.experiment2;

import java.util.ArrayList;
import java.util.List;

public class NodeListContainer{
	int numWay=0;
	List<AbstractNodeList<?>> nodeListList=new ArrayList<AbstractNodeList<?>>();
	
	public NodeListContainer(int numWay) {
		super();
		this.numWay = numWay;
	}

	public <T extends Node> void addNodeList(AbstractNodeList<T> nodeList){
		for(T node: nodeList.getNodeList()){
			node.setNumWay(numWay);
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
	
	public int getNumWay() {
		return numWay;
	}

	public void setNumWay(int numWay) {
		this.numWay = numWay;
	}

	public List<AbstractNodeList<?>> getNodeListList() {
		return nodeListList;
	}

	public void setNodeListList(List<AbstractNodeList<?>> nodeListList) {
		this.nodeListList = nodeListList;
	}

	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("=================== NodeList2 (%d)[%d] ====================\n", numWay, nodeListList.size()));
		for(AbstractNodeList<?> n: nodeListList){
			sb.append(n.toString());
			sb.append("\n");
		}
		return sb.toString();
	}
}
