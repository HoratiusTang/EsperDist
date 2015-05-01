package dist.esper.experiment2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class NodeList{
	public List<EventPropOpType> typeList=new ArrayList<EventPropOpType>(5);
	public List<NodeList> filterNodeLists=new ArrayList<NodeList>(5);
	public List<Node> nodeList=new ArrayList<Node>();
	public List<Node> orderedNodeList=new ArrayList<Node>();
	
	public NodeList(){
	}
	
	public NodeList(EventPropOpType type) {
		super();
		this.typeList.add(type);
	}
	
	public NodeList(List<EventPropOpType> typeList) {
		super();
		this.typeList = typeList;
	}
	
	public int getTypeCount(){
		return typeList.size();
	}
	
	public EventPropOpType getType(int index){
		return typeList.get(index);
	}
	
	public boolean isEventExisted(int eventType){
		for(EventPropOpType type: typeList){
			if(type.eventType == eventType){
				return true;
			}
		}
		return false;
	}
	
	public void addType(EventPropOpType type){
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
