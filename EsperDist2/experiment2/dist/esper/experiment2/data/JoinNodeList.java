package dist.esper.experiment2.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JoinNodeList extends AbstractNodeList<JoinNode>{	
	List<FilterEventPropOpType> filterTypeList=new ArrayList<FilterEventPropOpType>(5);
	List<JoinPropOpType> joinPropOpList=new ArrayList<JoinPropOpType>(4);
	List<JoinFilterNodeList> filterNodeLists=new ArrayList<JoinFilterNodeList>(5);
	
	public JoinNodeList(){
	}
	
	public JoinNodeList(FilterEventPropOpType type) {
		super();
		this.filterTypeList.add(type);
	}
	
	public JoinNodeList(List<FilterEventPropOpType> typeList) {
		super();
		this.filterTypeList = typeList;
	}
	
	public int getTypeCount(){
		return filterTypeList.size();
	}
	
	public FilterEventPropOpType getType(int index){
		return filterTypeList.get(index);
	}
	
	public boolean isEventExisted(int eventType){
		for(FilterEventPropOpType type: filterTypeList){
			if(type.eventType == eventType){
				return true;
			}
		}
		return false;
	}
//	public void addJoinPropOp(JoinPropOpType joinPropOp){
//		joinPropOpList.add(joinPropOp);
//	}
	
	public List<JoinPropOpType> getJoinPropOpList(){
		return this.joinPropOpList;
	}
	public void addFilterType(FilterEventPropOpType type){
		filterTypeList.add(type);
	}
	
	public void setFilterNodesList(List<JoinFilterNodeList> filterNodeLists){
		this.filterNodeLists = filterNodeLists;
	}
	
	public List<FilterEventPropOpType> getFilterTypeList() {
		return filterTypeList;
	}

	public void setFilterTypeList(List<FilterEventPropOpType> filterTypeList) {
		this.filterTypeList = filterTypeList;
	}

	public List<JoinFilterNodeList> getFilterNodeLists() {
		return filterNodeLists;
	}

	public void setFilterNodeLists(List<JoinFilterNodeList> filterNodeLists) {
		this.filterNodeLists = filterNodeLists;
	}

	public void setJoinPropOpList(List<JoinPropOpType> joinPropOpList) {
		this.joinPropOpList = joinPropOpList;
	}

	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(String.format("------------------ NodeList %s  ------------------\n", filterTypeList.toString()));
		for(Node n: nodeList){
			sb.append(n.toString());
			sb.append("\n");
		}
		return sb.toString();
	}
}
