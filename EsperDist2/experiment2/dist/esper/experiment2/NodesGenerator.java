package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;

public class NodesGenerator {
	static int MAX_WAYS=5;

	public int numEventTypes=10;
	public int numPropTypes=6;//6*2, '<' or '>'
	public int numNodePerType=20;
	
	public int[] n=new int[MAX_WAYS+1];//ignore [0]
	public double[] e=new double[MAX_WAYS+1];//ignore [0], 0-1
	public int m0;//only for filter
	public int[] m=new int[MAX_WAYS+1];//ignore [0][1], chained node count
	
	EventPropOpTypeCountRandomChooser chooser=new EventPropOpTypeCountRandomChooser();
	NodeRandomSorter nodeSorter=new NodeRandomSorter();
	Random rand=new Random();
	NodeList2[] nodeList2s=new NodeList2[MAX_WAYS+1];
	
	public List<Node> genearteNodeList(){		
		for(int i=1;i<MAX_WAYS;i++){
			generateNWayNodeList2(i);
		}
		
		return null;
	}
	
	public void generateNWayNodeList2(int numWay){
		if(numWay==1){
			generateFilterNodeList();
		}
		//int totalCount=computeNWayNodeTotalCount(numWay);
		int totalCount=n[numWay];
		NodeList2 nl2=new NodeList2(numWay);
		
		int curCount=0;
		while(curCount<totalCount){
			chooser.reset();
			NodeList nl=new NodeList();
			while(nl.getTypeCount()<numWay){
				EventPropOpType type=chooser.next();
				if(!nl.isEventExisted(type.eventType)){
					nl.addType(type);
				}
			}
			int nodesCount=getNormalRandomNumber(numNodePerType/numWay);
			JoinNode[] jns=new JoinNode[nodesCount];
			
			int eqCount=(int)(nodesCount*e[numWay]);
			int neqCount=nodesCount-eqCount;//0-neqCount++

			List<NodeList> fnLists=new ArrayList<NodeList>(nl.getTypeCount());
			for(int j=0;j<numWay;j++){
				NodeList fnList=new NodeList(nl.getType(j));
				fnList.addNode(new FilterNode(nl.getType(j)));
				fnLists.add(fnList);
			}
			
			/* init first */
			jns[0]=new JoinNode();
			for(int j=0;j<numWay;j++){
				jns[0].addUpNode(fnLists.get(j).getLastNode());
			}
			
			for(int i=0; i<neqCount; i++){
				jns[i]=new JoinNode();
				int k=rand.nextInt(numWay);
				fnLists.get(k).addNode(new FilterNode(nl.getType(k)));
				for(int j=0; j<numWay; j++){
					jns[i].addUpNode(fnLists.get(j).getLastNode());
				}
			}
			
			for(int i=neqCount; i<nodesCount; i++){
				int k=rand.nextInt(neqCount);
				jns[i].setTag(jns[k].getTag());
				jns[i].setUpNodeList(jns[k].getUpNodeList());
			}
			nodeSorter.randomSort(jns, m[numWay], 0.5d);
			
			nl.setNodes(jns);
			nl.setFilterNodesList(fnLists);
			nl2.addNodeList(nl);
			curCount+=jns.length;
		}
		nodeList2s[1]=nl2;
	}
	
	public void generateFilterNodeList(){
		int totalFilterCount=n[1];
		int curFilterCount=0;
		NodeList2 nodeList2=new NodeList2(1);
		while(curFilterCount < totalFilterCount){
			EventPropOpType type=chooser.next();
			int typeCount=getNormalRandomNumber(numNodePerType);
			FilterNode[] fns=new FilterNode[typeCount];
			for(int i=0; i<typeCount; i++){
				FilterNode fn=new FilterNode(type);
				fns[i]=fn;
			}
			int eqCount=(int)(typeCount*e[1]);
			int neqCount=typeCount-eqCount;
			for(int i=neqCount; i<typeCount; i++){
				int j=rand.nextInt(neqCount);
				fns[i].setTag(fns[j].getTag());
			}
			nodeSorter.randomSort(fns, m0, 0.5d);
			//completeFitlerNodeRelations(fns);
			NodeList nodeList=new NodeList(type);
			nodeList.setNodes(fns);
			nodeList2.addNodeList(nodeList);
			curFilterCount+=fns.length;
		}
		nodeList2s[1]=nodeList2;
	}
	
//	private void completeFitlerNodeRelations(FilterNode[] fns){
//		for(int i=0; i<fns.length; i++){
//			fns[i].setColumn(i);
//			for(int j=0; j<i;  j++){
//				if(fns[i].getTag() > fns[j].getTag()){
//					fns[i].addImplyNode(fns[j]);
//				}
//				else if(fns[i].getTag()==fns[j].getTag()){
//					fns[i].addEqualNode(fns[j]);
//				}
//			}
//		}
//	}
	
	class NodeList{
		List<EventPropOpType> typeList=new ArrayList<EventPropOpType>(5);
		List<NodeList> filterNodeLists=new ArrayList<NodeList>(5);
		public List<Node> nodes=new ArrayList<Node>();
		
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
					return false;
				}
			}
			return true;
		}
		
		public void addType(EventPropOpType type){
			typeList.add(type);
		}

		public <T extends Node> void setNodes(T[] nodes){
			this.nodes=(List<Node>)Arrays.asList(nodes);
		}
		
		public void addNode(Node node){
			this.nodes.add(node);
		}
		
		public int getNodesCount(){
			return nodes.size();
		}
		
		public Node getNode(int index){
			return nodes.get(index);
		}
		
		public Node getLastNode(){
			if(nodes.size()>0){
				return nodes.get(nodes.size()-1);
			}
			return null;
		}
		
		public void setFilterNodesList(List<NodeList> filterNodeLists){
			this.filterNodeLists = filterNodeLists;
		}
	}
	
	class NodeList2{
		int numWay=0;
		List<NodeList> nodeListList=new ArrayList<NodeList>();
		
		public NodeList2(int numWay) {
			super();
			this.numWay = numWay;
		}

		public void addNodeList(NodeList nodeList){
			int row=nodeListList.size();			
			for(Node node: nodeList.nodes){
				node.setNumWay(numWay);
				node.setRow(row);
			}
			nodeListList.add(nodeList);
		}
		
		public int getNodeListsCount(){
			return nodeListList.size();
		}
		
		public int getNodesTotalCount(){
			int count=0;
			for(NodeList nodeList: nodeListList){
				count+=nodeList.getNodesCount();
			}
			return count;
		}
	}
	
//	private int computeNWayNodeTotalCount(int numWay){
//		int totalCount=0;
//		for(int i=2; i<MAX_WAYS; i++){
//			if(i==numWay){
//				totalCount+=n[i];
//			}
//			else{
//				if(i==numWay/2){
//					totalCount+=n[i];
//				}
//				if(i==numWay-numWay/2){
//					totalCount+=n[i];
//				}
//			}
//		}
//		return totalCount;
//	}
	
	public int getNormalRandomNumber(double expect){
		return getNormalRandomNumber(expect, 1.0d, expect/2.0, Integer.MAX_VALUE);
	}
	
	public int getNormalRandomNumber(double expect, double std, double min, double max){
		double num=rand.nextGaussian()*std+expect;
		if(num<min)
			return (int)min;		
		if(num>max)
			return (int)max;		
		return (int)num;
	}
	
	class EventPropOpTypeCountRandomChooser{
		public double std=1.0; //standard devition
		public Set<Integer> set=new TreeSet<Integer>();
		
		public EventPropOpType next(){
			int et, pt, op;
			EventPropOpType type;
			while(true){
				et=rand.nextInt(numEventTypes);
				pt=rand.nextInt(numPropTypes);
				op=rand.nextInt(2);
				type=new EventPropOpType(et, pt, op);
				if(!set.contains(type.hashCode())){					
					return type;
				}
			}
		}
		
		public void reset(){
			set.clear();
		}
	}
	
	
}

//public int n1=100;
//public double e1=0.8;
//public double m1=3;
//
//public int n2=50;
//public double e2=0.8;
//public double m2=3;
//
//public int n3=0;
//public double e3=0.8;
//public double m3=3;
//
//public int n4=0;
//public double e4=0.8;
//public double m4=3;
//
//public int n5=0;
//public double e5=0.8;
//public double m5=3;
