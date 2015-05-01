package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;

public class NodesGenerator {
	public static final int MAX_WAYS=5;

	public int numEventTypes=10;
	public int numPropTypes=6;
	//public int numNodePerType=20;

	NodesParameter[] nodeParams=new NodesParameter[MAX_WAYS+1];
	EventPropOpTypeCountRandomChooser chooser=new EventPropOpTypeCountRandomChooser();
	NodeRandomSorter nodeSorter=new NodeRandomSorter();
	Random rand=new Random();
	NodeList2[] nodeList2s=new NodeList2[MAX_WAYS+1];
	
	public NodesGenerator(int numEventTypes, int numPropTypes,
			NodesParameter[] nps) {
		super();
		this.numEventTypes = numEventTypes;
		this.numPropTypes = numPropTypes;
		//this.numNodePerType = numNodePerType;
		for(NodesParameter np: nps){
			if(np.numWay>0 && np.numWay<=MAX_WAYS){
				nodeParams[np.numWay]=np;
			}
		}
	}

	public NodeList2[] genearteNodeList2s(){
		for(int i=1;i<=MAX_WAYS;i++){
			if(nodeParams[i]!=null){
				System.out.format("generate %d-way nodes starting...\n",i);
				generateNWayNodeList2(i);
				System.out.format("generate %d-way nodes finished\n",i);
			}
		}
		return nodeList2s;
	}
	
	public void generateNWayNodeList2(int numWay){
		if(numWay==1){
			generateFilterNodeList();
			return;
		}
		NodeList2 nl2=new NodeList2(numWay);
		
		int curCount=0;
		while(curCount<nodeParams[numWay].nodeCount){
			chooser.reset();
			NodeList nl=new NodeList();
			while(nl.getTypeCount()<numWay){
				EventPropOpType type=chooser.next();
				if(!nl.isEventExisted(type.eventType)){
					nl.addType(type);
				}
			}
			//int count=getNormalRandomNumber(nodeParams[numWay].nodeCountPerType);
			int count=nodeParams[numWay].nodeCountPerType;
			JoinNode[] jns=new JoinNode[count];
			
			int eqCount=(int)(count*nodeParams[numWay].equalRatio);
			int neqCount=count-eqCount;//0-neqCount++

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
			
			for(int i=1; i<neqCount; i++){
				jns[i]=new JoinNode();
				int k=rand.nextInt(numWay);
				fnLists.get(k).addNode(new FilterNode(nl.getType(k)));
				for(int j=0; j<numWay; j++){
					jns[i].addUpNode(fnLists.get(j).getLastNode());
				}
			}
			
			for(int i=neqCount; i<count; i++){
				jns[i]=new JoinNode();
				int k=rand.nextInt(neqCount);
				jns[i].setTag(jns[k].getTag());
				jns[i].setUpNodeList(jns[k].getUpNodeList());
			}
			nl.copySortedNodes(jns);
			nodeSorter.randomSort(jns, nodeParams[numWay].implyRatio, nodeParams[numWay].implyRatio/5.0);
			nl.setNodes(jns);
			nl.setFilterNodesList(fnLists);
			nl2.addNodeList(nl);
			curCount+=jns.length;
		}
		nodeList2s[numWay]=nl2;
	}
	
	public void generateFilterNodeList(){
		int curCount=0;
		NodeList2 nl2=new NodeList2(1);
		while(curCount < nodeParams[1].nodeCount){
			EventPropOpType type=chooser.next();
			NodeList nl=new NodeList(type);
			//int count=getNormalRandomNumber(nodeParams[1].nodeCountPerType);
			int count=nodeParams[1].nodeCountPerType;
			FilterNode[] fns=new FilterNode[count];
			for(int i=0; i<count; i++){
				FilterNode fn=new FilterNode(type);
				fns[i]=fn;
			}
			int eqCount=(int)(count*nodeParams[1].equalRatio);
			int neqCount=count-eqCount;
			for(int i=neqCount; i<count; i++){
				int j=rand.nextInt(neqCount);
				fns[i].setTag(fns[j].getTag());
			}
			nl.copySortedNodes(fns);
			nodeSorter.randomSort(fns, nodeParams[1].implyRatio, nodeParams[1].implyRatio/5.0);
			nl.setNodes(fns);
			nl2.addNodeList(nl);
			curCount+=fns.length;
		}
		nodeList2s[1]=nl2;
	}
	
	
	public int getNormalRandomNumber(double expect){
		return getNormalRandomNumber(expect, 1.0d, (expect+1.0)/2.0, Integer.MAX_VALUE);
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