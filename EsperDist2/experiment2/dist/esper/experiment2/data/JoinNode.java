package dist.esper.experiment2.data;

import java.util.*;

public class JoinNode extends Node {
	List<JoinPropOpType> joinPropOpList=new ArrayList<JoinPropOpType>(4);
	List<FilterNode> filterNodeList=new ArrayList<FilterNode>(5);
	
	public JoinNode() {
	}
	public JoinNode(List<JoinPropOpType> joinPropOpList) {
		super();
		this.joinPropOpList = joinPropOpList;
	}

	public void addFilterNode(FilterNode n){
		if(!filterNodeList.contains(n)){
			filterNodeList.add(n);
		}
	}
	public List<FilterNode> getFilterNodeList() {
		return filterNodeList;
	}
	public void setFilterNodeList(List<FilterNode> filterNodeList) {
		this.filterNodeList = filterNodeList;
	}
	
	public List<JoinPropOpType> getJoinPropOpList() {
		return joinPropOpList;
	}
	public void setJoinPropOpList(List<JoinPropOpType> joinPropOpList) {
		this.joinPropOpList = joinPropOpList;
	}	
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("JN(%d-%d-%d): ", id, tag, filterNodeList.size()));
		
		for(int i=0; i<filterNodeList.size(); i++){
			sb.append(filterNodeList.get(i).toString());
			if(i<filterNodeList.size()-1){
				JoinPropOpType p=joinPropOpList.get(i);
				sb.append(String.format(" ~ %d-%d ~ ", p.propType, p.opType));
			}
		}
		return sb.toString();
	}
}
