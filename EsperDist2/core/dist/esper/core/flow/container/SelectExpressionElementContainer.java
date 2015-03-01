package dist.esper.core.flow.container;


import java.util.*;

import dist.esper.epl.expr.AbstractResultExpression;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.util.DeepCloneFactory;

/**
 * the container to hold equivalent @SelectClauseExpressionElement(s) which come
 * from different @Stream(s). 
 * used in all concrete @StreamContainer(s)
 * 
 * @author tjy
 *
 */
public class SelectExpressionElementContainer extends
		SelectClauseExpressionElement {
	private static final long serialVersionUID = -4525394626426913772L;
	List<SelectClauseExpressionElement> selectElementList=new ArrayList<SelectClauseExpressionElement>(4);
	
	public SelectExpressionElementContainer() {
		super();
	}

	public SelectExpressionElementContainer(SelectClauseExpressionElement se) {
		this.setUniqueName(se.getUniqueName());
		this.setSelectExpr((AbstractResultExpression)DeepCloneFactory.staticDeepClone(se.getSelectExpr()));
		this.selectElementList.add(se);
	}
	
	public void setUniqueName(String uniqueName) {
		super.setUniqueName(uniqueName);
		for(SelectClauseExpressionElement selectElment: selectElementList){
			selectElment.setUniqueName(uniqueName);
		}
	}

	public List<SelectClauseExpressionElement> getSelectElementList() {
		return selectElementList;
	}

	public void setSelectElementList(
			List<SelectClauseExpressionElement> selectElementList) {
		this.selectElementList = selectElementList;
	}
	
//	@Override
//	public void toStringBuilder(StringBuilder sw) {
//		this.getSelectExpr().toStringBuilder(sw);
//		sw.append(this.getUniqueName()!=null?this.getUniqueName():"null");
//		if(this.getAssigndName()!=null){
//			sw.append(" as ");
//			sw.append(this.getAssigndName());
//		}
//	}
}
