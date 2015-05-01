package dist.esper.experiment2;

import java.util.*;

public class JoinNode extends Node {
	List<Node> upNodeList=new ArrayList<Node>(5);
	public JoinNode() {
	}
	public void addUpNode(Node n){
		if(!upNodeList.contains(n)){
			upNodeList.add(n);
		}
	}
	public List<Node> getUpNodeList() {
		return upNodeList;
	}
	public void setUpNodeList(List<Node> upNodeList) {
		this.upNodeList = upNodeList;
	}
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("JN(%d-%d-%d): ", id, tag, upNodeList.size()));
		for(Node n: upNodeList){
			sb.append(n.toString());
			sb.append(", ");
		}
		if(upNodeList.size()>0){
			sb.setLength(sb.length()-2);
		}
		return sb.toString();
	}
}
