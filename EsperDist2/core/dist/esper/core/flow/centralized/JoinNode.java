package dist.esper.core.flow.centralized;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.util.StringUtil;

/**
 * the join node in @Tree.
 * usually contains two child nodes in childList
 *  
 * @author tjy
 *
 */
public class JoinNode extends Node{
	ArrayList<Node> childList=new ArrayList<Node>(2);
	List<AbstractBooleanExpression> joinExprList=new ArrayList<AbstractBooleanExpression>(2);

	public JoinNode(){
		super();
	}
	
	public JoinNode(Node... nodes){
		for(Node node: nodes){
			childList.add(node);
		}
		this.eplId=nodes[0].getEplId();
	}
	
	public void addFilterExprssion(AbstractBooleanExpression joinExpr){
		joinExprList.add(joinExpr);
	}
	
	public void addChild(Node child){
		childList.add(child);
	}
	
	public ArrayList<Node> getChildList() {
		return childList;
	}

	public void setChildList(ArrayList<Node> childList) {
		this.childList = childList;
	}

	public List<AbstractBooleanExpression> getJoinExprList() {
		return joinExprList;
	}

	public void setJoinExprList(List<AbstractBooleanExpression> joinExprList) {
		this.joinExprList = joinExprList;
	}

	@Override
	public void dumpSelectedEventAliases(Set<EventAlias> eaSet) {
		for(Node node: childList){
			node.dumpSelectedEventAliases(eaSet);
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		NodeStringlizer.toStringBuilder(this, sw, indent);
	}

	@Override
	public int getLevel() {
		int childMaxLevel=0;
		for(Node node: childList){
			int childLevel=node.getLevel();
			childMaxLevel=(childLevel>childMaxLevel)?childLevel:childMaxLevel;
		}
		return childMaxLevel+1;
	}
}
