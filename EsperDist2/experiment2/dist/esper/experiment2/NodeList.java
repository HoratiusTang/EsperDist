package dist.esper.experiment2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NodeList{	
	public List<FilterEventPropOpType> typeList=new ArrayList<FilterEventPropOpType>(5);
	public List<JoinPropOpType> joinPropOpList=new ArrayList<JoinPropOpType>(4);
	public List<NodeList> filterNodeLists=new ArrayList<NodeList>(5);
	public List<Node> nodeList=new ArrayList<Node>();
	public List<Node> orderedNodeList=new ArrayList<Node>();
	public int popIndex;
	
	public NodeList(){
	}
	
	public NodeList(FilterEventPropOpType type) {
		super();
		this.typeList.add(type);
	}
	
	public NodeList(List<FilterEventPropOpType> typeList) {
		super();
		this.typeList = typeList;
	}
	
	public int getTypeCount(){
		return typeList.size();
	}
	
	public FilterEventPropOpType getType(int index){
		return typeList.get(index);
	}
	
	public boolean isEventExisted(int eventType){
		for(FilterEventPropOpType type: typeList){
			if(type.eventType == eventType){
				return true;
			}
		}
		return false;
	}
	public void addJoinPropOp(JoinPropOpType joinPropOp){
		joinPropOpList.add(joinPropOp);
	}
	public List<JoinPropOpType> getJoinPropOpList(){
		return this.joinPropOpList;
	}
	public void addType(FilterEventPropOpType type){
		typeList.add(type);
	}

	public <T extends Node> void setNodes(T[] nodes){
		this.nodeList=(List<Node>)Arrays.asList(nodes);
	}
	
	public void addNode(Node node){
		this.nodeList.add(node);
	}
	
	public int getNodesCount(){
		return nodeList.size();
	}
	
	public Node getNode(int index){
		return nodeList.get(index);
	}
	
	public Node getLastNode(){
		if(nodeList.size()>0){
			return nodeList.get(nodeList.size()-1);
		}
		return null;
	}
	
	public void setFilterNodesList(List<NodeList> filterNodeLists){
		this.filterNodeLists = filterNodeLists;
	}
	
	public <T extends Node> void copySortedNodes(T[] nodes){
		T[] orderedNodes=nodes.clone();
		this.orderedNodeList=(List<Node>)Arrays.asList(orderedNodes);
	}
	
	public void resetPop(){
		this.popIndex=0;
	}
	
	public Node popNode(){
		if(popIndex < nodeList.size()){
			Node n=nodeList.get(popIndex);
			popIndex++;
			return n;
		}
		return null;
	}
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("------------------ NodeList %s  ------------------\n", typeList.toString()));
		for(Node n: nodeList){
			sb.append(n.toString());
			sb.append("\n");
		}
		return sb.toString();
	}
}
