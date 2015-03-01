package dist.esper.monitor.ui.layout;

import org.eclipse.zest.layouts.algorithms.AbstractLayoutAlgorithm;
import org.eclipse.zest.layouts.dataStructures.InternalNode;
import org.eclipse.zest.layouts.dataStructures.InternalRelationship;

public class EmptyLayoutAlgorithm extends AbstractLayoutAlgorithm {

	public EmptyLayoutAlgorithm(int styles) {
		super(styles);		
	}

	@Override
	public void setLayoutArea(double x, double y, double width, double height) {
	}

	@Override
	protected boolean isValidConfiguration(boolean asynchronous,
			boolean continuous) {
		return true;
	}

	@Override
	protected void applyLayoutInternal(InternalNode[] entitiesToLayout,
			InternalRelationship[] relationshipsToConsider, double boundsX,
			double boundsY, double boundsWidth, double boundsHeight) {
	}

	@Override
	protected void preLayoutAlgorithm(InternalNode[] entitiesToLayout,
			InternalRelationship[] relationshipsToConsider, double x, double y,
			double width, double height) {
	}

	@Override
	protected void postLayoutAlgorithm(InternalNode[] entitiesToLayout,
			InternalRelationship[] relationshipsToConsider) {
	}

	@Override
	protected int getTotalNumberOfLayoutSteps() {		
		return 0;
	}

	@Override
	protected int getCurrentLayoutStep() {		
		return 0;
	}

}
