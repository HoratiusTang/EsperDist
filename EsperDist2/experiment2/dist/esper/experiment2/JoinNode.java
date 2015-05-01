package dist.esper.experiment2;

import java.util.*;

public class JoinNode extends Node {
	public List<JoinPropOpType> joinPropOpList=new ArrayList<JoinPropOpType>(4);
	List<Node> upNodeList=new ArrayList<Node>(5);
	List<SelectElement> selectElementList=new ArrayList<SelectElement>(10);
	public JoinNode() {
	}
	public JoinNode(List<JoinPropOpType> joinPropOpList) {
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
	
	public List<JoinPropOpType> getJoinPropOpList() {
		return joinPropOpList;
	}
	public void setJoinPropOpList(List<JoinPropOpType> joinPropOpList) {
		this.joinPropOpList = joinPropOpList;
	}
	
	public void addSelectElement(SelectElement se){
		selectElementList.add(se);
	}
	
	public List<SelectElement> getSelectElementList() {
		return selectElementList;
	}
	public void setSelectElementList(List<SelectElement> selectElementList) {
		this.selectElementList = selectElementList;
	}
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("JN(%d-%d-%d): ", id, tag, upNodeList.size()));
		
		for(int i=0; i<upNodeList.size(); i++){
			sb.append(upNodeList.get(i).toString());
			if(i<upNodeList.size()-1){
				JoinPropOpType p=joinPropOpList.get(i);
				sb.append(String.format("~ %d-%d ~", p.propType, p.opType));
			}
		}
		return sb.toString();
	}
}
