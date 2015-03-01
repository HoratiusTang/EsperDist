package dist.esper.core.flow.centralized;

import java.util.ArrayList;
import java.util.List;

import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.ViewSpecification;

/**
 * the super-class of @FilterNode and @PatternNode, 
 * it contains view specifications by users.
 * 
 * @author tjy
 *
 */
public abstract class BaseNode extends Node{
	String optionalStreamName;
	ViewSpecification[] viewSpecs;
	public BaseNode(){
		super();
	}
	public String getOptionalStreamName() {
		return optionalStreamName;
	}
	public void setOptionalStreamName(String optionalStreamName) {
		this.optionalStreamName = optionalStreamName;
	}
	public ViewSpecification[] getViewSpecs() {
		return viewSpecs;
	}
	public void setViewSpecs(ViewSpecification[] viewSpecs) {
		this.viewSpecs = viewSpecs;
	}
	
	@Override
	public int getLevel() {
		return 1;
	}
}
