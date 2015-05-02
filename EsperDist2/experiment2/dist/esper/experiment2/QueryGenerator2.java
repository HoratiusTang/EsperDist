package dist.esper.experiment2;

import java.util.*;

import dist.esper.core.util.NumberFormatter;
import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.external.event.*;

public class QueryGenerator2 {
	EventInstanceGenerator[] eigs;
	int numEventTypes;
	int numPropTypes;
	OperatorTypeEnum[] filterOpTypes;
	OperatorTypeEnum[] joinOpTypes;
	int[] windowTimes;
	int numSelectElementsPerFilter;
	NodesParameter[] nodeParams;
	NodeListContainer[] nodeListCnts;
	List<Node> allNodeList;
	List<String> queryList;
	NodesGenerator nodesGen;
	QueryBuilder queryBuilder=new QueryBuilder();
	NumberComparator numberComparator=new NumberComparator();
	
	public QueryGenerator2(EventInstanceGenerator[] eigs,
			OperatorTypeEnum[] filterOpTypes, OperatorTypeEnum[] joinOpTypes,
			int[] windowTimes, int numSelectElementsPerFilter, 
			NodesParameter[] nodeParams) {
		super();
		this.eigs = eigs;
		this.filterOpTypes = filterOpTypes;
		this.joinOpTypes = joinOpTypes;
		this.windowTimes = windowTimes;
		this.numSelectElementsPerFilter = numSelectElementsPerFilter;
		this.nodeParams = nodeParams;
		
		this.numEventTypes = eigs.length;
		this.numPropTypes = eigs[0].getEvent().getPropList().size();
	}

	public List<String> generateQueries(){
		generateNodeList2s();
		generateValues();
		mergeAllNodes();
		buildQueries();
		return queryList;
	}
	
	public void generateNodeList2s(){
		nodesGen=new NodesGenerator(numEventTypes, numPropTypes, 
				filterOpTypes.length, joinOpTypes.length,
				windowTimes.length, numSelectElementsPerFilter, nodeParams);
		nodeListCnts=nodesGen.genearteNodeList2s();
	}
	
	public void generateValues(){
		for(int i=0; i<nodeListCnts.length; i++){
			if(nodeListCnts[i]!=null){
				NodeListContainer nl2=nodeListCnts[i];
				for(int j=0; j<nl2.getNodeListsCount(); j++){
					System.out.format("begin generateValues(): [%d][%d]\n", i, j);
					generateValues(nl2.nodeListList.get(j));
					System.out.format("end   generateValues(): [%d][%d]\n", i, j);
				}
			}
		}
	}
	
	public void generateValues(AbstractNodeList<?> nl){
		if(nl instanceof FilterNodeList){//filter
			FilterNodeList fnl=(FilterNodeList)nl;
			FilterEventPropOpType filterType=fnl.getFilterType();
			EventInstanceGenerator eig=eigs[filterType.eventType];
			FieldGenerator fg=eig.getFieldGeneratorByIndex(filterType.propType);
			
			Number[] nums=this.generateSortedNumbers(fg, filterType.opType, nl.getNonEqualNodeCount());
			int i;
			for(i=0; i<nl.getNonEqualNodeCount(); i++){
				FilterNode fn=(FilterNode)nl.getOrderedNodeByIndex(i);
				fn.setValue(nums[i]);
			}
			for( ; i<nl.getNodesCount(); i++){
				FilterNode fn=(FilterNode)nl.getOrderedNodeByIndex(i);
				FilterNode fn0=(FilterNode)searchNodeWithId(fnl.getOrderedNodeList(), fn.getTag());
				fn.setValue(fn0.getValue());
			}
		}
		else{
			JoinNodeList jnl=(JoinNodeList)nl;
			for(FilterNodeList fnl: jnl.getFilterNodeLists()){
				generateValues(fnl);
			}
		}
	}
	
	public void mergeAllNodes(){
		Random rand=new Random();
		int nodeTotalCount=0;
		LinkedList<List<? extends Node>> nodeLists=new LinkedList<List<? extends Node>>();
		LinkedList<Index> nodePositions=new LinkedList<Index>();
		for(NodeListContainer nl2: nodeListCnts){
			if(nl2!=null){
				for(AbstractNodeList<?> nl: nl2.getNodeListList()){
					nodeLists.add(nl.getNodeList());
					nodePositions.add(new Index(0));
					nodeTotalCount+=nl.getNodesCount();
				}
			}
		}
		
		allNodeList=new ArrayList<Node>(nodeTotalCount);
		int nodeListIndex;
		List<? extends Node> nodeList;
		Index nodePosition;
		while(allNodeList.size() < nodeTotalCount){
			nodeListIndex=rand.nextInt(nodeLists.size());
			nodeList=nodeLists.get(nodeListIndex);
			nodePosition=nodePositions.get(nodeListIndex);
			if(nodePosition.index < nodeList.size()){
				allNodeList.add(nodeList.get(nodePosition.index));
				nodePosition.index++;
			}
			else{
				nodeLists.remove(nodeListIndex);
				nodePositions.remove(nodeListIndex);
			}
		}
	}
	
	public void buildQueries(){
		queryList=new ArrayList<String>(allNodeList.size());
		for(int i=0; i<allNodeList.size(); i++){
			Node n=allNodeList.get(i);
			String q;
			if(n instanceof FilterNode){
				q=queryBuilder.buildFilterQuery((FilterNode)n);
			}
			else{
				q=queryBuilder.buildJoinQuery((JoinNode)n);
			}
			queryList.add(q);
		}
	}
	
	class QueryBuilder{
		int count=0;
		public String buildFilterQuery(FilterNode fn){
			String aliasName=buildQueryEventAliasName(fn, 1);
			List<String> seStrList=buildFilterSelectElementStrings(fn, aliasName);
			String fromStr=buildFromElementString(fn, aliasName, false);
			String query=assembleQuery(seStrList, Arrays.asList(new String[]{fromStr}), null);
			count++;
			return query;
		}
		public String buildJoinQuery(JoinNode jn){
			List<String> aliasNameList=buildQueryEventAliasNames(jn.getFilterNodeList());
			List<String> seStrList=buildJoinSelectElementStrings(jn, aliasNameList);
			List<String> jcStrList=buildJoinConditionStrings(jn, aliasNameList);
			List<String> fromStrList=buildFromElementStrings(jn.getFilterNodeList(), aliasNameList);
			String query=assembleQuery(seStrList, fromStrList, jcStrList);
			count++;
			return query;
		}
		
		public List<String> buildJoinConditionStrings(JoinNode jn, List<String> aliasNameList){
			List<String> jcList=new ArrayList<String>(jn.getJoinPropOpList().size());
			for(int i=0; i<jn.getJoinPropOpList().size(); i++){
				JoinPropOpType jpo=jn.getJoinPropOpList().get(i);
				FilterNode lfn=jn.getFilterNodeList().get(i);
				FilterNode rfn=jn.getFilterNodeList().get(i+1);
				String lpropName=buildAliasDotPropString(lfn, aliasNameList.get(i), jpo.propType);
				String rpropName=buildAliasDotPropString(rfn, aliasNameList.get(i+1), jpo.propType);
				OperatorTypeEnum opType=joinOpTypes[jpo.opType];
				String jc=String.format("%s%s%s", lpropName, opType.getString(), rpropName);
				jcList.add(jc);
			}
			return jcList;
		}
		
		public List<String> buildFilterSelectElementStrings(FilterNode fn, String aliasName){
			List<String> seStrList=new ArrayList<String>(fn.getSelectElementList().size());
			for(SelectElement se: fn.getSelectElementList()){
				String seStr=buildAliasDotPropString(
						fn,
						aliasName,
						se.propType);
				seStrList.add(seStr);
			}
			return seStrList;
		}
		
		public List<String> buildJoinSelectElementStrings(JoinNode jn, List<String> aliasNameList){
			List<String> seStrList=new ArrayList<String>(jn.getSelectElementList().size());
			for(SelectElement se: jn.getSelectElementList()){
				String seStr=buildAliasDotPropString(
						jn.getFilterNodeList().get(se.filterNodeIndex),
						aliasNameList.get(se.filterNodeIndex),
						se.propType);
				seStrList.add(seStr);
			}
			return seStrList;
		}
		
		public String buildAliasDotPropString(FilterNode fn, String aliasName, int propType){
			String seStr=String.format("%s.%s", aliasName, getPropName(fn.getFilterType().eventType, propType));
			return seStr;
		}
		
		private String getPropName(int eventType, int propType){
			EventInstanceGenerator eig=eigs[eventType];
			String propName=eig.getPropertyNameByIndex(propType);
			return propName;
		}
		
		public List<String> buildFromElementStrings(List<FilterNode> fnList, List<String> aliasNameList){
			List<String> fromStrList=new ArrayList<String>(fnList.size());
			for(int i=0; i<fnList.size(); i++){
				String fromStr=buildFromElementString(fnList.get(i), aliasNameList.get(i), true);
				fromStrList.add(fromStr);
			}
			return fromStrList;
		}
		
		//FIXME: multi filter condition
		public String buildFromElementString(FilterNode fn, String aliasName, boolean withWindow){
			EventInstanceGenerator eig=eigs[fn.getFilterType().eventType];
			String propName=getPropName(fn.getFilterType().eventType, fn.getFilterType().propType);
			OperatorTypeEnum opType=filterOpTypes[fn.getFilterType().opType];
			int windowTime=windowTimes[fn.getFilterType().windowType];
			String fromElementStr;
			if(withWindow){
				fromElementStr=String.format("%s(%s%s%s).win:time(%d sec) as %s",
						eig.getEventName(), propName, opType.getString(),
						NumberFormatter.format(fn.getValue()), 
						windowTime, aliasName);
			}
			else{
				fromElementStr=String.format("%s(%s%s%s) as %s",
						eig.getEventName(), propName, opType.getString(),
						NumberFormatter.format(fn.getValue()), aliasName);
			}
			return fromElementStr;
		}
		
		public List<String> buildQueryEventAliasNames(List<FilterNode> fnList){
			List<String> eaList=new ArrayList<String>(fnList.size());
			for(int i=0; i<fnList.size(); i++){
				FilterNode fn=fnList.get(i);
				String aliasName=buildQueryEventAliasName(fn, i);
				eaList.add(aliasName);
			}
			return eaList;
		}
		
		public String buildQueryEventAliasName(FilterNode fn, int fnIndex){
			String eventName=eigs[fn.getFilterType().eventType].getEventName();
			String aliasName=String.format("%s%04d%02d", 
					eventName.toLowerCase(), count, fnIndex);
			return aliasName;
		}
		
		public String assembleQuery(List<String> seStrList, List<String> fromStrList, List<String> jcStrList){
			StringBuilder sb=new StringBuilder();
			String dem=" ";
			sb.append("SELECT");
			for(String seStr: seStrList){
				sb.append(dem);
				sb.append(seStr);
				dem=", ";
			}
			sb.append("\n");
			dem=" ";
			sb.append("FROM");
			for(String fromStr: fromStrList){
				sb.append(dem);
				sb.append(fromStr);
				dem=", ";
			}
			if(jcStrList!=null && jcStrList.size()>0){
				sb.append("\n");
				dem=" ";
				sb.append("WHERE");
				for(String jcStr: jcStrList){
					sb.append(dem);
					sb.append(jcStr);
					dem=" and ";
				}
			}
			sb.append("\n");
			String query=sb.toString();
			return query;
		}
	}
	
	private Number[] generateSortedNumbers(FieldGenerator fg, int filterOpType, int count){
		Number[] nums=new Number[count];
		for(int i=0; i<count; i++){
			nums[i]=(Number)fg.random();
		}
		if(filterOpTypes[filterOpType]==OperatorTypeEnum.GREATER ||
				filterOpTypes[filterOpType]==OperatorTypeEnum.GREATER_OR_EQUAL){
			numberComparator.setAscOrder(true);
		}
		else{
			numberComparator.setAscOrder(false);
		}
		Arrays.sort(nums, numberComparator);
		return nums;
	}
	
	private Number getNextValue(Number lastVal, FieldGenerator fg, int filterOpType){
		boolean isGreater=false;
		if(filterOpTypes[filterOpType]==OperatorTypeEnum.GREATER ||
				filterOpTypes[filterOpType]==OperatorTypeEnum.GREATER_OR_EQUAL){
			isGreater=true;
		}
		Number nextVal=null;
		while(true){
			nextVal=(Number)fg.random();
			if((lastVal==null || nextVal.doubleValue() > lastVal.doubleValue()) 
					&& isGreater){
				break;
			}
		}
		return nextVal;
	}
	
	private <T extends Node> T searchNodeWithId(List<T> nodeList, int tag){
		for(T n: nodeList){
			if(n.getId()==tag){
				return n;
			}
		}
		return null;
	}
	
	class Index{
		int index=0;
		public Index(int index){
			this.index = index;
		}
		@Override
		public String toString(){
			return ""+index;
		}
	}
	
	class NumberComparator implements Comparator<Number>{
		boolean asc=true;
		@Override
		public int compare(Number n1, Number n2) {
			double diff=n1.doubleValue()-n2.doubleValue();
			if(diff==0.0){
				return 0;
			}
			else if(diff>0){
				return asc?1:-1;
			}
			else{
				return asc?-1:1;
			}
		}
		
		public void setAscOrder(boolean asc){
			this.asc = asc;
		}
	}
}
