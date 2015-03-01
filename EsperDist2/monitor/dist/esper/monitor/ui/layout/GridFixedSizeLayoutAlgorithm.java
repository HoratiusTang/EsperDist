package dist.esper.monitor.ui.layout;

import org.eclipse.zest.layouts.algorithms.AbstractLayoutAlgorithm;
import org.eclipse.zest.layouts.dataStructures.InternalNode;
import org.eclipse.zest.layouts.dataStructures.InternalRelationship;

public class GridFixedSizeLayoutAlgorithm extends EmptyLayoutAlgorithm{
	int nodeWidth=40;
	int nodeHeight=30;
	public GridFixedSizeLayoutAlgorithm(int styles) {
		super(styles);		
	}
	
	public int getNodeWidth() {
		return nodeWidth;
	}

	public void setNodeWidth(int nodeWidth) {
		this.nodeWidth = nodeWidth;
	}

	public int getNodeHeight() {
		return nodeHeight;
	}

	public void setNodeHeight(int nodeHeight) {
		this.nodeHeight = nodeHeight;
	}

	@Override
	protected void applyLayoutInternal(InternalNode[] nodes,
			InternalRelationship[] relationshipsToConsider, double boundsX,
			double boundsY, double boundsWidth, double boundsHeight) {
		assert(boundsWidth>nodeWidth);
		int numColumns=(int) (boundsWidth/nodeWidth);
		
		for(int i=0;i<nodes.length;i++){
			int rowIndex=i/numColumns;
			int colIndex=i-(rowIndex*numColumns);
			nodes[i].setLocation(colIndex*nodeWidth, rowIndex*nodeHeight);
			nodes[i].setSize(nodeWidth, nodeHeight);
		}
	}
	
	@Override
	protected int getTotalNumberOfLayoutSteps() {
		return 1;
	}

	@Override
	protected int getCurrentLayoutStep() {
		return 0;
	}

}
