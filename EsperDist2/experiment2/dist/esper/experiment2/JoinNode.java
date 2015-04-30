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
}
