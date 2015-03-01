package dist.esper.epl.expr;


import java.util.*;

import com.espertech.esper.client.soda.*;
import com.espertech.esper.epl.expression.ExprNode;
import com.espertech.esper.epl.spec.GroupByClauseExpressions;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.expr.util.Stringlizable;
import dist.esper.epl.sementic.IResolvable;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class GroupByClause extends AbstractClause{
	//AbstractIdentExpression[] groupByNodes=null;
	List<AbstractIdentExpression> groupByExprList=new ArrayList<AbstractIdentExpression>(2);
	
	public GroupByClause(){
		super();
		this.type="group by";
	}
//	public GroupByClause(AbstractIdentExpression[] groupByNodes) {
//		this();
//		this.groupByNodes = groupByNodes;
//	}
	
	public void addExpression(AbstractIdentExpression expr){
		groupByExprList.add(expr);
	}
	
	public List<AbstractIdentExpression> getGroupByExprList() {
		return groupByExprList;
	}

	public void setGroupByExprList(List<AbstractIdentExpression> groupByExprList) {
		this.groupByExprList = groupByExprList;
	}

	public static class Factory{
		public static GroupByClause make(GroupByClauseExpressions gbc0){
			if(gbc0==null){
				return null;
			}
//			ExprNode[] gyns=gbc0.getGroupByNodes();
//			AbstractIdentExpression[] groupByNodes=new AbstractIdentExpression[gbce.getGroupByNodes().length];			
//			for(int i=0;i<gyns.length;i++){
//				groupByNodes[i]=(AbstractIdentExpression)ExpressionFactory.toExpression(gyns[i]);
//			}
//			GroupByClause gyc=new GroupByClause(groupByNodes);
			GroupByClause gbc1=new GroupByClause();
			for(ExprNode gbn0: gbc0.getGroupByNodes()){
				gbc1.addExpression((AbstractIdentExpression)ExpressionFactory.toExpression(gbn0));
			}
			return gbc1;
		}
		
		public static GroupByClause make(com.espertech.esper.client.soda.GroupByClause gbc0){
			GroupByClause gbc1=new GroupByClause();
			for(GroupByClauseExpression gbe0: gbc0.getGroupByExpressions()){
				assert(gbe0 instanceof GroupByClauseExpressionSingle);
				AbstractIdentExpression gbe1=(AbstractIdentExpression)ExpressionFactory1.toExpression1(((GroupByClauseExpressionSingle)gbe0).getExpression());
				gbc1.addExpression(gbe1);
			}
			return gbc1;
		}
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw){
		sw.append(type);
		String delimitor=" ";
		for(AbstractIdentExpression gbn: groupByExprList){
			sw.append(delimitor);
			gbn.toStringBuilder(sw);
			delimitor=", ";
		}
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
//		for(int i=0;i<groupByNodes.length;i++){
//			if(!groupByNodes[i].resolve(ssw,param)){
//				AbstractIdentExpression newExpr=(AbstractIdentExpression)ExpressionFactory.resolve(groupByNodes[i],ssw, param);
//				groupByNodes[i]=newExpr;
//			}
//		}
		for(int i=0;i<groupByExprList.size();i++){
			if(!groupByExprList.get(i).resolve(ssw,param)){
				AbstractIdentExpression newExpr=(AbstractIdentExpression)ExpressionFactory.resolve(groupByExprList.get(i),ssw, param);
				groupByExprList.set(i, newExpr);
			}
		}
		return true;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitGroupByClause(this);
	}
}
