package dist.esper.experiment2.data;

import java.util.*;

public abstract class AbstractNodeList<T extends Node> {
	public List<T> nodeList=new ArrayList<T>();
	public List<T> orderedNodeList=new ArrayList<T>();
	public int nonEqualNodeCount;
	public List<T> getNodeList() {
		return nodeList;
	}
	public void setNodeList(List<T> nodeList) {
		this.nodeList = nodeList;
	}
	public List<T> getOrderedNodeList() {
		return orderedNodeList;
	}
	public void setOrderedNodeList(List<T> orderedNodeList) {
		this.orderedNodeList = orderedNodeList;
	}
	public int getNonEqualNodeCount() {
		return nonEqualNodeCount;
	}
	public void setNonEqualNodeCount(int nonEqualNodeCount) {
		this.nonEqualNodeCount = nonEqualNodeCount;
	}
	public int getNodesCount(){
		return nodeList.size();
	}
	public Node getNodeByIndex(int index){
		return nodeList.get(index);
	}
	public Node getOrderedNodeByIndex(int index){
		return orderedNodeList.get(index);
	}
	public void setNodes(T[] nodes){
		this.nodeList=Arrays.asList(nodes);
	}
	public void copySortedNodes(T[] nodes){
		T[] orderedNodes=nodes.clone();
		this.orderedNodeList=Arrays.asList(orderedNodes);
	}
}
