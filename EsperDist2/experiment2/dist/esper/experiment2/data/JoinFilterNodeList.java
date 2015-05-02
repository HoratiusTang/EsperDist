package dist.esper.experiment2.data;

import java.util.List;

public class JoinFilterNodeList extends FilterNodeList {

	public JoinFilterNodeList(FilterEventPropOpType filterType) {
		super(filterType);
	}
	
	public void addNode(FilterNode fn){
		this.nodeList.add(fn);
	}
	
	public Node getLastNode(){
		if(nodeList.size()>0){
			return nodeList.get(nodeList.size()-1);
		}
		return null;
	}
	
	@Override
	public List<FilterNode> getOrderedNodeList() {
		return nodeList;
	}
	
	@Override
	public Node getOrderedNodeByIndex(int index){
		return nodeList.get(index);
	}
	
	@Override
	public void setOrderedNodeList(List<FilterNode> orderedNodeList) {
		this.nodeList = orderedNodeList;
	}
}
