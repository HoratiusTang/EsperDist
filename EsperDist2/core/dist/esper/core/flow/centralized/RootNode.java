package dist.esper.core.flow.centralized;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.util.StringUtil;

/**
 * the top-most node in @Tree.
 * its resultElementList is the final output.
 * 
 * @author tjy
 *
 */
public class RootNode extends Node {
	Node child=null;
	
	/**if the child is a PatternNode, the whereExprList may not be empty*/
	List<AbstractBooleanExpression> whereExprList=new ArrayList<AbstractBooleanExpression>(2);
	
	public RootNode(Node child) {
		super();
		this.setChild(child);
	}

	public RootNode(Node child,
			List<SelectClauseExpressionElement> resultElementList) {
		super();
		this.child = child;
		this.resultElementList = resultElementList;
	}

	public Node getChild() {
		return child;
	}

	public void setChild(Node child) {
		this.child = child;
		this.eplId = child.getEplId();
	}
	
	public void addWhereExprssion(AbstractBooleanExpression whereExpr){
		whereExprList.add(whereExpr);
	}
	
	public List<AbstractBooleanExpression> getWhereExprList() {
		return whereExprList;
	}
	
	public void setFilterExprList(List<AbstractBooleanExpression> whereExprList) {
		this.whereExprList.addAll(whereExprList);
	}

	@Override
	public void dumpSelectedEventAliases(Set<EventAlias> eaSet) {
		child.dumpSelectedEventAliases(eaSet);
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		NodeStringlizer.toStringBuilder(this, sw, indent);
	}
	
	@Override
	public int getLevel() {
		return child.getLevel()+1;
	}
}
