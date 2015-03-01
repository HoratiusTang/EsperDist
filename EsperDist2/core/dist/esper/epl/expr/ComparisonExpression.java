package dist.esper.epl.expr;


import java.util.ArrayList;
import java.util.List;

import com.espertech.esper.client.soda.Expression;
import com.espertech.esper.client.soda.RelationalOpExpression;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.ComparisonJsonSerializer.class)
public class ComparisonExpression extends AbstractBooleanExpression{
	private static final long serialVersionUID = 8297043259091807797L;
	OperatorTypeEnum relation=OperatorTypeEnum.NONE;
	ArrayList<AbstractResultExpression> childExprList=null;
	
	public ComparisonExpression() {
		super();
	}

	public ComparisonExpression(OperatorTypeEnum relation) {
		super();
		this.relation = relation;
		childExprList=new ArrayList<AbstractResultExpression>(2);
	}
	
	public ComparisonExpression(OperatorTypeEnum relation,
			ArrayList<AbstractResultExpression> childExprList) {
		super();
		this.relation = relation;
		this.childExprList = childExprList;
	}
	
	public void addExpression(AbstractResultExpression expr){
		childExprList.add(expr);
	}

	public OperatorTypeEnum getRelation() {
		return relation;
	}

	public void setRelation(OperatorTypeEnum relation) {
		this.relation = relation;
	}

	public ArrayList<AbstractResultExpression> getChildExprList() {
		return childExprList;
	}

	public void setChildExprList(ArrayList<AbstractResultExpression> childExprList) {
		this.childExprList = childExprList;
	}
	
	public AbstractResultExpression getChild(int index){
		return childExprList.get(index);
	}

	/**
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append("(");
		childExprList.get(0).toStringBuilder(sw);
		sw.append(relation.toString());
		childExprList.get(1).toStringBuilder(sw);
		sw.append(")");
	}
	*/
	
	/**
	@Override
	public void toStringBuilderWithNickName(StringBuilder sw) {
		sw.append("(");
		childExprList.get(0).toStringBuilderWithNickName(sw);
		sw.append(relation.toString());
		childExprList.get(1).toStringBuilderWithNickName(sw);
		sw.append(")");
	}
	*/
	
	/**
	@Override
	public int eigenCode() {
		int code=relation.ordinal();
		for(AbstractPropertyExpression expr: childExprList){
			code ^= expr.eigenCode();
		}
		return code;
	}
	*/

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		for(int i=0;i<childExprList.size();i++){
			if(!childExprList.get(i).resolve(ssw,param)){
				AbstractResultExpression newExpr=(AbstractResultExpression)ExpressionFactory.resolve(childExprList.get(i),ssw, param);
				childExprList.set(i, newExpr);
			}
		}
		AbstractResultExpression first=childExprList.get(0);
		AbstractResultExpression second=childExprList.get(1);
		//make EventOrPropertySpecification prior to Value
		if(first.getClass().getSimpleName().compareTo(second.getClass().getSimpleName())>0){
			childExprList.set(0, second);
			childExprList.set(1, first);
			relation=relation.reverse();
		}
		return true;
	}
	
	public void reverse(){
		AbstractResultExpression first=childExprList.get(0);
		AbstractResultExpression second=childExprList.get(1);
		childExprList.set(0, second);
		childExprList.set(1, first);
		relation=relation.reverse();
	}

	/**
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		for(AbstractPropertyExpression child: childExprList){
			child.dumpAllEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		for(AbstractPropertyExpression child: childExprList){
			child.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	*/
	
	@Override
	public void dumpConjunctionExpressions(List<AbstractBooleanExpression> conjunctList){
		conjunctList.add(this);
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitComparisionExpression(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitComparisionExpression(this, obj);
	}

	@Override
	public int getComparisonExpressionCount() {
		return 1;
	}
	
	public static class Factory{
		public static ComparisonExpression make(RelationalOpExpression ce0){
			OperatorTypeEnum op=OperatorTypeEnum.Factory.valueOf(ce0.getOperator());
			ComparisonExpression ce1=new ComparisonExpression(op);
			
			for(Expression child0: ce0.getChildren()){
				AbstractResultExpression child1=(AbstractResultExpression)ExpressionFactory1.toExpression1(child0);
				ce1.addExpression(child1);
			}
			return ce1;
		}
	}
}
