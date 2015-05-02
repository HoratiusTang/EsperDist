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
	NodeList2[] nodeList2s;
	List<Node> allNodeList;
	List<String> queryList;
	QueryBuilder queryBuilder=new QueryBuilder();
	
	public List<String> generateQueries(){
		generateNodeList2s();
		generateValues();
		mergeAllNodes();
		buildQueries();
		return queryList;
	}
	
	public void generateNodeList2s(){
		
	}
	
	public void generateValues(){
		for(NodeList2 nl2: nodeList2s){
			if(nl2!=null){
				for(NodeList nl: nl2.nodeListList){
					generateValues(nl);
				}
			}
		}
	}
	
	public void generateValues(NodeList nl){
		if(nl.getTypeCount()==1){//filter
			FilterEventPropOpType filterType=nl.getType(0);
			EventInstanceGenerator eig=eigs[filterType.eventType];
			FieldGenerator fg=eig.getFieldGeneratorByIndex(filterType.propType);
			Number lastVal=null;
			for(Node n: nl.orderedNodeList){
				FilterNode fn=(FilterNode)n;
				if(fn.getTag()==fn.getId()){
					lastVal=getNextValue(lastVal, fg, filterType.opType);
					fn.setValue(lastVal);
				}
				else{
					FilterNode fn0=(FilterNode)searchNodeWithId(nl.nodeList, fn.getTag());
					fn.setValue(fn0.getValue());
				}
			}
		}
		else{//join
			for(NodeList fnl: nl.filterNodeLists){
				generateValues(fnl);
			}
		}
	}
	
	public void mergeAllNodes(){
		Random rand=new Random();
		int nodeTotalCount=0;
		LinkedList<List<Node>> nodeLists=new LinkedList<List<Node>>();
		LinkedList<Index> nodePositions=new LinkedList<Index>();
		for(NodeList2 nl2: nodeList2s){
			if(nl2!=null){
				for(NodeList nl: nl2.nodeListList){
					nodeLists.add(nl.nodeList);
					nodePositions.add(new Index());
					nodeTotalCount+=nl.getNodesCount();
				}
			}
		}
		
		allNodeList=new ArrayList<Node>(nodeTotalCount);
		int nodeListIndex;
		List<Node> nodeList;
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
			String aliasName=buildQueryEventAliasName(fn);
			List<String> seStrList=buildFilterSelectElementStrings(fn, aliasName);
			String fromStr=buildFromElementString(fn, aliasName);
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
				String rpropName=buildAliasDotPropString(lfn, aliasNameList.get(i+i), jpo.propType);
				OperatorTypeEnum opType=filterOpTypes[jpo.opType];
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
				String fromStr=buildFromElementString(fnList.get(i), aliasNameList.get(i));
				fromStrList.add(fromStr);
			}
			return fromStrList;
		}
		
		//FIXME: multi filter condition
		public String buildFromElementString(FilterNode fn, String aliasName){
			EventInstanceGenerator eig=eigs[fn.getFilterType().eventType];
			String propName=getPropName(fn.getFilterType().eventType, fn.getFilterType().propType);
			OperatorTypeEnum opType=filterOpTypes[fn.getFilterType().opType];
			String fromElementStr=String.format("%s(%s%s%s) as %s",
					eig.getEvent(), propName, opType.getString(),
					NumberFormatter.format(fn.getValue()), aliasName);
			return fromElementStr;
		}
		
		public List<String> buildQueryEventAliasNames(List<FilterNode> fnList){
			List<String> eaList=new ArrayList<String>(fnList.size());
			for(int i=0; i<fnList.size(); i++){
				FilterNode fn=fnList.get(i);
				String eventName=eigs[fn.getFilterType().eventType].getEventName();
				String aliasName=String.format("%s%04d%02d", 
						eventName.toLowerCase(), count, i);
				eaList.add(aliasName);
			}
			return eaList;
		}
		
		public String buildQueryEventAliasName(FilterNode fn){
			String eventName=eigs[fn.getFilterType().eventType].getEventName();
			String aliasName=String.format("%s%04d%02d", 
					eventName.toLowerCase(), count, 1);
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
			dem=" ";
			sb.append("FROM");
			for(String fromStr: fromStrList){
				sb.append(dem);
				sb.append(fromStr);
				dem=", ";
			}
			if(jcStrList!=null && jcStrList.size()>0){
				dem=" ";
				sb.append("WHERE");
				for(String jcStr: jcStrList){
					sb.append(dem);
					sb.append(jcStr);
					dem=" and ";
				}
			}
			String query=sb.toString();
			return query;
		}
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
	
	private Node searchNodeWithId(List<Node> nodeList, int tag){
		for(Node n: nodeList){
			if(n.getId()==tag){
				return n;
			}
		}
		return null;
	}
	
	class Index{
		int index=0;
	}
}
