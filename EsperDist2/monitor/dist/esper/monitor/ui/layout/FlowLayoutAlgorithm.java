package dist.esper.monitor.ui.layout;

import java.util.Arrays;

import org.eclipse.zest.layouts.dataStructures.InternalNode;
import org.eclipse.zest.layouts.dataStructures.InternalRelationship;

public class FlowLayoutAlgorithm extends EmptyLayoutAlgorithm {
	int marginX=30;
	int marginY=30;
	int spaceX=60;
	int spaceY=30;
	int nodeWidth=50;
	int nodeHeight=30;
	
	public FlowLayoutAlgorithm(int styles) {
		super(styles);		
	}
	@Override
	protected void applyLayoutInternal(InternalNode[] nodes, InternalRelationship[] edges, 
			double boundsX, double boundsY, double boundsWidth, double boundsHeight) {
		int[] nie=new int[nodes.length];//number of in-edge for each node
		int[] level=new int[nodes.length];
		//boolean[] edgeFlag=new boolean[edges.length];
		Arrays.fill(nie,0);
		Arrays.fill(level,-1);
		//Arrays.fill(edgeFlag,false);
		InternalRelationship[][] outEdges=new InternalRelationship[nodes.length][];
		for(int i=0;i<nodes.length;i++){
			outEdges[i]=countOutEdges(nodes[i], edges);
			nie[i]=countInEdges(nodes[i], edges);
			if(nie[i]==0){
				level[i]=0;
			}
		}
		
		for(int i=0;i<nodes.length;i++){
			if(level[i]==0){
				traverseAlongEdge(level, i, nodes, outEdges);
			}
		}
		
		int maxLevel=0;
		for(int i=0;i<nodes.length;i++){
			maxLevel=level[i]>maxLevel?level[i]:maxLevel;
		}
		
		int[] levelNodeCount=new int[maxLevel+1];
		int[] levelNodeCurCount=new int[maxLevel+1];
		Arrays.fill(levelNodeCount, 0);
		Arrays.fill(levelNodeCurCount, 0);
		for(int i=0;i<nodes.length;i++){
			levelNodeCount[level[i]]++;
		}
		
		int maxRowCount=0;
		InternalNode[][] levelNodes=new InternalNode[maxLevel+1][];
		for(int i=0;i<=maxLevel;i++){
			levelNodes[i]=new InternalNode[levelNodeCount[i]];
			maxRowCount=Math.max(maxRowCount, levelNodeCount[i]);
		}
		
		for(int i=0;i<nodes.length;i++){
			levelNodes[level[i]][levelNodeCurCount[level[i]]]=nodes[i];
			levelNodeCurCount[level[i]]++;
		}
		
		fireProgressStarted(getTotalNumberOfLayoutSteps()); 
		for(int i=0;i<levelNodes.length;i++){
			for(int j=0;j<levelNodes[i].length;j++){
				InternalNode node=levelNodes[i][j];
				int[] location=computeLocation(i, j, levelNodes[i].length, maxRowCount);
				node.setLocation(location[0], location[1]);
				node.setSize(nodeWidth, nodeHeight);
			}
		}
		fireProgressEvent(0, getTotalNumberOfLayoutSteps());
        fireProgressEnded(getTotalNumberOfLayoutSteps());
	}
	@Override
	protected int getTotalNumberOfLayoutSteps() {		
		return 0;
	}
	
	private int[] computeLocation(int col, int row, int curRowCount, int maxRowCount){//begin at 0
		int maxSpanY= maxRowCount*nodeHeight + (maxRowCount-1)*spaceY;
		int curSpanY= curRowCount*nodeHeight + (curRowCount-1)*spaceY;
		int offsetY= (maxSpanY-curSpanY)/2;
		int x=(nodeWidth+spaceX)*col;
		int y=(nodeHeight+spaceY)*row + offsetY;
		return new int[]{x+marginX, y+marginY};
	}
	
	private void traverseAlongEdge(int[] level, int beginIndex, 
			InternalNode[] nodes, 
			InternalRelationship[][] outEdges){
		for(InternalRelationship outEdge: outEdges[beginIndex]){
			InternalNode nextNode=outEdge.getDestination();
			int nextIndex=getIndex(nodes, nextNode);
			level[nextIndex]=Math.max(level[nextIndex], level[beginIndex]+1);;
			traverseAlongEdge(level, nextIndex, nodes, outEdges);
		}
	}
	
	private int getIndex(InternalNode[] nodes, InternalNode node){
		for(int i=0;i<nodes.length;i++){
			if(nodes[i]==node){
				return i;
			}
		}
		return -1;
	}
	
	private int countInEdges(InternalNode node, InternalRelationship[] edges){
		int count=0;
		for(InternalRelationship edge: edges){
			if(edge.getDestination()==node){
				count++;
			}
		}
		return count;
	}
	
	private InternalRelationship[] countOutEdges(InternalNode node, InternalRelationship[] edges){
		int count=0;
		for(InternalRelationship edge: edges){
			if(edge.getSource()==node){
				count++;
			}
		}
		InternalRelationship[] outEdges=new InternalRelationship[count];
		int i=0;
		for(InternalRelationship edge: edges){
			if(edge.getSource()==node){
				outEdges[i]=edge;
				i++;
			}
		}
		return outEdges;
	}
}
