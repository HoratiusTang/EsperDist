package dist.esper.experiment2;

import java.util.*;

import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.experiment2.data.FilterEventPropOpType;
import dist.esper.experiment2.data.FilterNode;
import dist.esper.experiment2.data.FilterNodeList;
import dist.esper.experiment2.data.JoinFilterNodeList;
import dist.esper.experiment2.data.JoinNode;
import dist.esper.experiment2.data.JoinNodeList;
import dist.esper.experiment2.data.JoinPropOpType;
import dist.esper.experiment2.data.NodeListContainer;
import dist.esper.experiment2.data.NodesParameter;
import dist.esper.experiment2.data.SelectElement;

public class NodesGenerator {
	public static final int MAX_WAYS=5;

	public int numEventTypes=10;
	public int numPropTypes=6;
	public int numFilterOpTypes=2;
	public int numJoinOpTypes=3;
	public int numWindowTypes=3;
	public int numSelectElementsPerFilter=3;

	NodesParameter[] nodeParams=new NodesParameter[MAX_WAYS+1];
	FilterTypeRandomChooser ftRand=new FilterTypeRandomChooser();
	JoinTypeRandomChooser jtRand=new JoinTypeRandomChooser();
	NodeRandomSorter nodeSorter=new NodeRandomSorter();
	Random rand=new Random();
	IOpTypeValidator opTypeValidator=null;
	NodeListContainer[] nodeListCnts;
	
	public NodesGenerator(int numEventTypes, int numPropTypes,
			int numFilterOpTypes, int numJoinOpTypes, 
			int numWindowTypes, int numSelectElementsPerFilter,
			NodesParameter[] nps) {
		super();
		this.numEventTypes = numEventTypes;
		this.numPropTypes = numPropTypes;
		this.numFilterOpTypes = numFilterOpTypes;
		this.numJoinOpTypes = numJoinOpTypes;
		this.numWindowTypes = numWindowTypes;
		this.numSelectElementsPerFilter = numSelectElementsPerFilter;
		
		for(NodesParameter np: nps){
			if(np.numWay>0 && np.numWay<=MAX_WAYS){
				nodeParams[np.numWay]=np;
			}
		}
		reset();
	}
	
	public void reset(){
		ftRand.reset();
		nodeListCnts=new NodeListContainer[MAX_WAYS+1];
	}

	public NodeListContainer[] genearteNodeListContainers() throws Exception{
		for(int i=1;i<=MAX_WAYS;i++){
			if(nodeParams[i]!=null){
				//System.out.format("info: generate %d-way nodes starting...\n",i);
				int curRetries=0;
				while(true){
					try {
						generateJoinNodeListContainer(i);
						break;
					}
					catch (Exception e) {
						curRetries++;
						if(curRetries>10){
							throw new Exception(String.format("generateJoinNodeListContainer(%d) failed, already retried %d times", i, curRetries));
						}
//						System.out.format("error: generateJoinNodeListContainer(%d): %s, will retry the %dnd times\n", 
//								i, e.getMessage(), curRetries);
					}
				}
				//System.out.format("info: generate %d-way nodes finished\n",i);
			}
		}
		return nodeListCnts;
	}
	
	public void generateJoinNodeListContainer(int numWay) throws Exception{
		if(numWay==1){
			generateFilterNodeListContainer();
			return;
		}
		NodeListContainer nl2=new NodeListContainer(numWay);
		
		int curCount=0;
		while(curCount<nodeParams[numWay].nodeCount){
			int curRetries=0;
			while(true){
				try{
					ftRand.reset();
					JoinNodeList nl=new JoinNodeList();
					while(nl.getTypeCount()<numWay){
						FilterEventPropOpType filterType=ftRand.next(false);
						if(!nl.isEventExisted(filterType.eventType)){
							nl.addFilterType(filterType);
						}
					}
					nl.setJoinPropOpList(Arrays.asList(jtRand.next(numWay)));
					
					//int count=getNormalRandomNumber(nodeParams[numWay].nodeCountPerType);
					int count=nodeParams[numWay].nodeCountPerType;
					JoinNode[] jns=new JoinNode[count];
					
					int eqCount=(int)(count*nodeParams[numWay].equalRatio);
					int neqCount=count-eqCount;//0-neqCount++
					nl.setNonEqualNodeCount(neqCount);
		
					List<JoinFilterNodeList> fnLists=new ArrayList<JoinFilterNodeList>(nl.getTypeCount());
					for(int j=0;j<numWay;j++){
						JoinFilterNodeList fnList=new JoinFilterNodeList(nl.getType(j));
						fnList.addNode(new FilterNode(nl.getType(j)));
						fnLists.add(fnList);
					}
					
					/* init first */
					jns[0]=new JoinNode(nl.getJoinPropOpList());
					jns[0].setSelectElementList(genSelectElements(numWay));
					for(int j=0;j<numWay;j++){
						jns[0].addFilterNode((FilterNode)fnLists.get(j).getLastNode());
					}
					
					for(int i=1; i<neqCount; i++){
						jns[i]=new JoinNode(nl.getJoinPropOpList());
						jns[i].setSelectElementList(genSelectElements(numWay));
						int k=rand.nextInt(numWay);
						fnLists.get(k).addNode(new FilterNode(nl.getType(k)));
						for(int j=0; j<numWay; j++){
							jns[i].addFilterNode((FilterNode)fnLists.get(j).getLastNode());
						}
					}
					
					for(int j=0;j<numWay;j++){
						fnLists.get(j).setNonEqualNodeCount(fnLists.get(j).getNodesCount());
					}
					
					for(int i=neqCount; i<count; i++){
						jns[i]=new JoinNode(nl.getJoinPropOpList());
						jns[i].setSelectElementList(genSelectElements(numWay));
						int k=rand.nextInt(neqCount);
						jns[i].setTag(jns[k].getTag());
						jns[i].setFilterNodeList(jns[k].getFilterNodeList());
					}
					nl.copySortedNodes(jns);
					nodeSorter.randomSort(jns, nodeParams[numWay].implyRatio, 0.05);
					nl.setNodes(jns);
					nl.setFilterNodesList(fnLists);
					nl2.addNodeList(nl);
					curCount+=jns.length;
					break;
				}
				catch(Exception ex){
					//System.out.format("error: in generateJoinNodeListContainer(%d): %s", numWay, ex.getMessage());
					curRetries++;
					if(curRetries>50){
						throw ex;
					}
				}
			}
		}
		nodeListCnts[numWay]=nl2;
	}
	
	public void generateFilterNodeListContainer() throws Exception{
		int curCount=0;
		NodeListContainer nl2=new NodeListContainer(1);
		ftRand.reset();//it's a must! shit!
		while(curCount < nodeParams[1].nodeCount){
			int curRetries=0;
			while(true){
				try{
					FilterEventPropOpType filterType=ftRand.next(false);
					FilterNodeList nl=new FilterNodeList(filterType);
					//int count=getNormalRandomNumber(nodeParams[1].nodeCountPerType);
					int count=nodeParams[1].nodeCountPerType;
					FilterNode[] fns=new FilterNode[count];
					for(int i=0; i<count; i++){
						FilterNode fn=new FilterNode(filterType);
						fn.setSelectElementList(genSelectElements(1));
						fns[i]=fn;
					}
					int eqCount=(int)(count*nodeParams[1].equalRatio);
					int neqCount=count-eqCount;
					nl.setNonEqualNodeCount(neqCount);
					for(int i=neqCount; i<count; i++){
						int j=rand.nextInt(neqCount);
						fns[i].setTag(fns[j].getTag());
					}
					nl.copySortedNodes(fns);
					nodeSorter.randomSort(fns, nodeParams[1].implyRatio, 0.05);
					nl.setNodes(fns);
					nl2.addNodeList(nl);
					curCount+=fns.length;
					//System.out.format("curCount=%d\n", curCount);
					ftRand.storeFilterType(filterType);
					break;
				}
				catch(Exception ex){
					curRetries++;
					if(curRetries>50){
						throw ex;
					}
				}
			}
		}
		nodeListCnts[1]=nl2;
	}
	
	public List<SelectElement> genSelectElements(int numWay){
		int count=numWay * numSelectElementsPerFilter;
		List<SelectElement> seList=new ArrayList<SelectElement>(count);
		int filterNodeIndex, propType;
		for(int i=0; i<count; i++){
			filterNodeIndex = rand.nextInt(numWay);
			propType = rand.nextInt(numPropTypes);
			seList.add(new SelectElement(filterNodeIndex, propType));
		}
		return seList;
	}	
	
//	public int getNormalRandomNumber(double expect){
//		return getNormalRandomNumber(expect, 1.0d, (expect+1.0)/2.0, Integer.MAX_VALUE);
//	}
//	
//	public int getNormalRandomNumber(double expect, double std, double min, double max){
//		double num=rand.nextGaussian()*std+expect;
//		if(num<min)
//			return (int)min;		
//		if(num>max)
//			return (int)max;		
//		return (int)num;
//	}
	
	public IOpTypeValidator getOpTypeValidator() {
		return opTypeValidator;
	}

	public void setOpTypeValidator(IOpTypeValidator opTypeValidator) {
		this.opTypeValidator = opTypeValidator;
	}

	class FilterTypeRandomChooser{
		public double std=1.0; //standard devition
		public Set<Integer> set=new HashSet<Integer>();
		
		public FilterEventPropOpType next(boolean isStore){
			int eventType, propType, opType, windowType;
			FilterEventPropOpType filterType;
			while(true){
				eventType=rand.nextInt(numEventTypes);
				propType=rand.nextInt(numPropTypes);
				opType=rand.nextInt(numFilterOpTypes);
				windowType=rand.nextInt(numWindowTypes);
				filterType=new FilterEventPropOpType(eventType, propType, opType, windowType);
				if(!set.contains(filterType.hashCode())){
					if(opTypeValidator!=null){
						if(opTypeValidator.validateFilterOperation(filterType)){
							if(isStore){
								storeFilterType(filterType);
							}
							return filterType;
						}
					}
					else{
						return filterType;
					}
				}
			}
		}
		
		public void storeFilterType(FilterEventPropOpType filterType){
			set.add(filterType.hashCode());
		}
		public void reset(){
			set.clear();
		}
	}
	
	class JoinTypeRandomChooser{
		public JoinPropOpType[] next(int numWay){
			JoinPropOpType[] joinTypes=new JoinPropOpType[numWay-1];
			while(true){
				for(int i=0; i<numWay-1; i++){
					if(joinTypes[i]==null){
						joinTypes[i]=new JoinPropOpType();
					}
					joinTypes[i].propType=rand.nextInt(numPropTypes);
					joinTypes[i].opType=rand.nextInt(numJoinOpTypes);//>,<,=
				}
				if(opTypeValidator!=null){
					if(opTypeValidator.validateJoinOperation(joinTypes)){
						break;
					}
				}
				else{
					break;
				}
			}
			return joinTypes;
		}		
	}
	
	public static interface IOpTypeValidator{
		public boolean validateFilterOperation(FilterEventPropOpType filterType);
		public boolean validateJoinOperation(JoinPropOpType[] joinTypes);
	}
}