package dist.esper.experiment2;

import java.util.*;

public class JoinNode extends Node {
	public List<PropOpType> joinPropOpList=new ArrayList<PropOpType>(4);
	List<Node> upNodeList=new ArrayList<Node>(5);
	
	public JoinNode() {
	}
	public JoinNode(List<PropOpType> joinPropOpList) {
		super();
		this.joinPropOpList = joinPropOpList;
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
	
	public List<PropOpType> getJoinPropOpList() {
		return joinPropOpList;
	}
	public void setJoinPropOpList(List<PropOpType> joinPropOpList) {
		this.joinPropOpList = joinPropOpList;
	}
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("JN(%d-%d-%d): ", id, tag, upNodeList.size()));
		
		for(int i=0; i<upNodeList.size(); i++){
			sb.append(upNodeList.get(i).toString());
			if(i<upNodeList.size()-1){
				PropOpType p=joinPropOpList.get(i);
				sb.append(String.format("~ %d-%d ~", p.propType, p.opType));
			}
		}
		return sb.toString();
	}
}
