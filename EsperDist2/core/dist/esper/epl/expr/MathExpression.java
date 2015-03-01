package dist.esper.epl.expr;


import java.util.ArrayList;

import com.espertech.esper.client.soda.ArithmaticExpression;
import com.espertech.esper.client.soda.Expression;
import com.espertech.esper.epl.expression.ExprMathNode;
import com.espertech.esper.epl.expression.ExprNode;
import com.espertech.esper.type.MathArithTypeEnum;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.MathJsonSerializer.class)
public class MathExpression extends AbstractResultExpression{
	private static final long serialVersionUID = 1575333878212090558L;
	ArrayList<AbstractResultExpression> childExprList=new ArrayList<AbstractResultExpression>(2);
	MathOperatorEnum opType=MathOperatorEnum.NONE;
	
	public MathExpression() {
		super();
	}

	public MathExpression(MathOperatorEnum opType) {
		super();
		this.opType = opType;
	}
	
	public void addExpression(AbstractResultExpression expr){
		childExprList.add(expr);
	}

	public ArrayList<AbstractResultExpression> getChildExprList() {
		return childExprList;
	}

	public MathOperatorEnum getOpType() {
		return opType;
	}

	public void setOpType(MathOperatorEnum opType) {
		this.opType = opType;
	}

	public void setChildExprList(ArrayList<AbstractResultExpression> childExprList) {
		this.childExprList = childExprList;
	}

	public MathOperatorEnum getOperationType() {
		return opType;
	}

	public void setOperationType(MathOperatorEnum opType) {
		this.opType = opType;
	}

	/*
	@Override
	public void toStringBuilder(StringBuilder sw) {
		childExprList.get(0).toStringBuilder(sw);
		for(int i=1;i<childExprList.size();i++){
			sw.append(opType.getString());
			childExprList.get(i).toStringBuilder(sw);
		}
	}
	*/
	
	/**
	@Override
	public void toStringBuilderWithNickName(StringBuilder sw) {
		assert(childExprList.size()==2);
		childExprList.get(0).toStringBuilderWithNickName(sw);
		sw.append(opType.getString());
		childExprList.get(1).toStringBuilderWithNickName(sw);
	}
	*/
	
	public static class Factory{
		public static MathExpression make(ExprMathNode mathNode){
			MathOperatorEnum opType=toMathOperatorEnum(mathNode.getMathArithTypeEnum());
			MathExpression me=new MathExpression(opType);
			for(ExprNode cn: mathNode.getChildNodes()){
				AbstractResultExpression ce=(AbstractResultExpression)ExpressionFactory.toExpression(cn);
				me.addExpression(ce);
			}
			return me;
		}
		
		public static MathExpression make(ArithmaticExpression ae0){
			MathOperatorEnum opType=MathOperatorEnum.Factory.valueOf(ae0.getOperator());
			MathExpression me=new MathExpression(opType);
			for(Expression expr0: ae0.getChildren()){
				AbstractResultExpression expr1=(AbstractResultExpression)ExpressionFactory1.toExpression1(expr0);
				me.addExpression(expr1);
			}
			return me;
		}
		
		public static MathOperatorEnum toMathOperatorEnum(MathArithTypeEnum mat){
			switch(mat){
			case ADD:
				return MathOperatorEnum.ADD;
			case SUBTRACT:
				return MathOperatorEnum.MINUS;
			case DIVIDE:
				return MathOperatorEnum.DIV;
			case MULTIPLY:
				return MathOperatorEnum.MUL;
			case MODULO:
				return MathOperatorEnum.MOD;
			}
			return MathOperatorEnum.NONE;
		}
	}
	
	/**
	@Override
	public int eigenCode() {
		int code=opType.ordinal();
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
			if(!childExprList.get(i).resolve(ssw,null)){
				AbstractResultExpression newExpr=(AbstractResultExpression)ExpressionFactory.resolve(childExprList.get(i),ssw,param);
				childExprList.set(i, newExpr);
			}
		}
		//FIXME:sort
		return true;
	}

	/**
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		for(AbstractPropertyExpression childExpr: childExprList){
			childExpr.dumpAllEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		for(AbstractPropertyExpression childExpr: childExprList){
			childExpr.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	*/
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitMathExpression(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitMathExpression(this, obj);
	}
}
