package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;

public class NodesGenerator {
	static int MAX_WAYS=5;

	public int numEventTypes=10;
	public int numPropTypes=6;//6*2, '<' or '>'
	public int numNodePerType=20;
	
	public int[] n=new int[MAX_WAYS+1];//ignore [0]
	public int[] e=new int[MAX_WAYS+1];//ignore [0]
	public int[] m=new int[MAX_WAYS+1];//ignore [0]
	
	EventPropOpTypeCountRandomChooser chooser=new EventPropOpTypeCountRandomChooser();
	NodeRandomSorter nodeSorter=new NodeRandomSorter();
	Random rand=new Random();
	
	public List<Node> genearteNodeList(){		
		List<List<FilterNode>> fnsList=generateFilterNodeList();
		
		return null;
	}
	
	public List<List<FilterNode>> generateFilterNodeList(){
		int totalFilterCount=0;//total number of filters;
		for(int i=1; i<MAX_WAYS; i++){
			totalFilterCount += n[i] * i;
		}
		
		int curFilterCount=0;
		int rowCount=0;
		List<List<FilterNode>> fnsList=new LinkedList<List<FilterNode>>();
		while(curFilterCount < totalFilterCount){
			EventPropOpTypeCount type=chooser.next();
			//List<FilterNode> fns=new LinkedList<FilterNode>();
			FilterNode[] fns=new FilterNode[type.count];
			for(int i=0; i<type.count; i++){
				FilterNode fn=new FilterNode(type.eventType, type.propType, type.opType);
				fns[i]=fn;
			}
			int eqCount=type.count*e[1];
			int neqCount=type.count-eqCount;
			for(int i=neqCount; i<type.count; i++){
				int j=rand.nextInt(neqCount);
				fns[i].setTag(fns[j].getTag());
			}
			nodeSorter.randomSort(fns, m[1], 0.5d);
//			List<FilterNode> fnList=new LinkedList<FilterNode>();
			completeFitlerNodeRelations(fns);
//			for(int i=0; i<fns.length; i++){
//				fns[i].setRow(rowCount);
//				fnList.add(fns[i]);
//			}			
//			fnsList.add(fnList);
			rowCount++;
		}
		return fnsList;
	}
	
	private void completeFitlerNodeRelations(FilterNode[] fns){
		for(int i=0; i<fns.length; i++){
			fns[i].setColumn(i);
			for(int j=0; j<i;  j++){
				if(fns[i].getTag() > fns[j].getTag()){
					fns[i].addImplyNode(fns[j]);
				}
				else if(fns[i].getTag()==fns[j].getTag()){
					fns[i].addEqualNode(fns[j]);
				}
			}
		}
	}
	
	class NWayNodes<T extends Node>{
		int numWay=0;
		List<List<T>> nodeListList=new LinkedList<List<T>>();
		public void addNodes(T[] nodes){
			List<T> nodeList=new LinkedList<T>();
			for(int i=0;i<nodes.length;i++){
				nodes[i].setNumWay(numWay);
				nodes[i].setRow(nodeListList.size());
			}
			nodeListList.add(nodeList);
		}
	}
	
	class EventPropOpTypeCountRandomChooser{
		public double std=1.0; //standard devition
		public Set<Integer> set=new TreeSet<Integer>();
		
		public EventPropOpTypeCount next(){
			int et, pt, op, count, key;
			while(true){
				et=rand.nextInt(numEventTypes);
				pt=rand.nextInt(numPropTypes);
				op=rand.nextInt(2);
				key=et*100+pt*10+op;
				if(!set.contains(key)){
					count=(int)(rand.nextGaussian()*1.0+numNodePerType);
					count=(count>numNodePerType/2)?count:numNodePerType/2;
					return new EventPropOpTypeCount(et, pt, op, count);
				}
			}
		}
	}
	
	class EventPropOpTypeCount{
		public int eventType;
		public int propType;
		public int opType;
		public int count;
		public EventPropOpTypeCount(int eventType, int propType, int opType, int count) {
			super();
			this.eventType = eventType;
			this.propType = propType;
			this.opType = opType;
			this.count = count;
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
