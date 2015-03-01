package dist.esper.epl.expr;


import java.util.ArrayList;
import java.util.List;

import com.espertech.esper.client.soda.Conjunction;
import com.espertech.esper.client.soda.Disjunction;
import com.espertech.esper.client.soda.Expression;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.CompositeJsonSerializer.class)
public class CompositeExpression extends AbstractBooleanExpression{
	private static final long serialVersionUID = 6835137763065617424L;
	RelationTypeEnum relation=RelationTypeEnum.NONE;
	List<AbstractBooleanExpression> childExprList=new ArrayList<AbstractBooleanExpression>(4);
	
	public CompositeExpression() {
		super();
	}

	public CompositeExpression(RelationTypeEnum relation) {
		super();
		this.relation = relation;		
	}
	
	public RelationTypeEnum getRelation() {
		return relation;
	}
	
	public void setRelation(RelationTypeEnum relation) {
		this.relation = relation;
	}
	
	public List<AbstractBooleanExpression> getChildExprList() {
		return childExprList;
	}
	
	public void addChildExpr(AbstractBooleanExpression childExpr){
		childExprList.add(childExpr);
	}

	public void setChildExprList(List<AbstractBooleanExpression> childExprList) {
		this.childExprList = childExprList;
	}
	
	public int getChildExprCount(){
		return this.childExprList.size();
	}
	
	public AbstractBooleanExpression getChildExpr(int index){
		return this.childExprList.get(index);
	}

	public CompositeExpression(RelationTypeEnum relation,
			List<AbstractBooleanExpression> childExprList) {
		super();
		this.relation = relation;
		this.childExprList = childExprList;
	}
	
	public CompositeExpression(List<AbstractBooleanExpression> childExprList) {
		super();
		this.relation = RelationTypeEnum.AND;
		this.childExprList = childExprList;
	}
	
	public void addExpression(AbstractBooleanExpression expr){
		childExprList.add(expr);
	}

	/**
	@Override
	public void toStringBuilder(StringBuilder sw) {
//		sw.append("(");
//		sw.append(childExprList.get(0).toString());
//		for(int i=1;i<childExprList.size();i++){
//			sw.append(" ");
//			sw.append(relation.toString());
//			sw.append(" ");
//			childExprList.get(i).toStringBuilder(sw);
//		}
//		sw.append(")");
		toStringBuilder(childExprList, relation, sw);
	}
	*/
	
	/**
	@Override
	public void toStringBuilderWithNickName(StringBuilder sw) {
		sw.append("(");
		sw.append(childExprList.get(0).toString());
		for(int i=1;i<childExprList.size();i++){
			sw.append(" ");
			sw.append(relation.toString());
			sw.append(" ");
			childExprList.get(i).toStringBuilderWithNickName(sw);
		}
		sw.append(")");
	}
	*/
	
	/**
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
		for(AbstractBooleanExpression childExpr: childExprList){
			childExpr.resolve(ssw,param);
		}
		//Collections.sort(childExprList, comparator);
		return true;
	}
	
	/**
	static class EigenCodeComparator implements Comparator<AbstractBooleanExpression>{
		@Override
		public int compare(AbstractBooleanExpression a, AbstractBooleanExpression b) {
			return Integer.bitCount(a.eigenCode()) - Integer.bitCount(b.eigenCode());
		}
	}
	*/

	/**
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		for(AbstractBooleanExpression child: childExprList){
			child.dumpAllEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		for(AbstractBooleanExpression child: childExprList){
			child.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	*/
	
	@Override
	public void dumpConjunctionExpressions(List<AbstractBooleanExpression> conjunctList){
		CompositeExpression compExpr=(CompositeExpression)this;
		if(compExpr.getRelation()==RelationTypeEnum.AND){
			for(AbstractBooleanExpression child: this.childExprList){
				child.dumpConjunctionExpressions(conjunctList);
			}
		}
		else{
			conjunctList.add(this);
		}
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitCompositeExpression(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitCompositeExpression(this, obj);
	}

	@Override
	public int getComparisonExpressionCount() {
		int count=0;
		for(AbstractBooleanExpression child: childExprList){
			count+=child.getComparisonExpressionCount();
		}
		return count;
	}
	
	public static class Factory{
		public static CompositeExpression make(Expression ce0){
			CompositeExpression ce1=new CompositeExpression();
			if(ce0 instanceof Conjunction){
				ce1.setRelation(RelationTypeEnum.AND);
			}
			else if(ce0 instanceof Disjunction){
				ce1.setRelation(RelationTypeEnum.OR);
			}
			else{//Not
				ce1.setRelation(RelationTypeEnum.NOT);
			}
			for(Expression child0: ce0.getChildren()){
				AbstractBooleanExpression child1=(AbstractBooleanExpression)ExpressionFactory1.toExpression1(child0);
				ce1.addChildExpr(child1);
			}
			return ce1;
		}
	}
}
