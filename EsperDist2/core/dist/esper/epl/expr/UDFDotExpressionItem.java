package dist.esper.epl.expr;


import java.util.ArrayList;

import com.espertech.esper.client.soda.DotExpressionItem;
import com.espertech.esper.client.soda.Expression;
import com.espertech.esper.epl.expression.ExprChainedSpec;
import com.espertech.esper.epl.expression.ExprNode;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.UCSJsonSerializer.class)
public class UDFDotExpressionItem extends AbstractResultExpression{
	private static final long serialVersionUID = 1447347396844492200L;
	String name=null;
	ArrayList<AbstractResultExpression> paramList=new ArrayList<AbstractResultExpression>(2);
	boolean property=false;
	
	public UDFDotExpressionItem() {
		super();
	}
	public UDFDotExpressionItem(String name) {
		super();
		this.name = name;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public boolean isProperty() {
		return property;
	}
	public boolean getProperty() {
		return property;
	}
	public void setProperty(boolean isProperty) {
		this.property = isProperty;
	}
	public ArrayList<AbstractResultExpression> getParamList() {
		return paramList;
	}
	
	public void setParamList(ArrayList<AbstractResultExpression> paramList) {
		this.paramList = paramList;
	}
	public void addParameter(AbstractResultExpression param){
		paramList.add(param);
	}
	
	public static class Factory{
		public static UDFDotExpressionItem make(ExprChainedSpec ecs){
			UDFDotExpressionItem ucs=new UDFDotExpressionItem(ecs.getName());
			ucs.setProperty(ecs.isProperty());
			for(ExprNode expr: ecs.getParameters()){
				AbstractResultExpression param=(AbstractResultExpression)ExpressionFactory.toExpression(expr);
				ucs.addParameter(param);
			}
			return ucs;
		}
		public static UDFDotExpressionItem make(DotExpressionItem item0){
			UDFDotExpressionItem item1=new UDFDotExpressionItem(item0.getName());
			item1.setProperty(item0.isProperty());
			for(Expression param0: item0.getParameters()){
				AbstractResultExpression param1=(AbstractResultExpression)ExpressionFactory1.toExpression1(param0);
				item1.addParameter(param1);
			}
			return item1;
		}
	}
	
	/*
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(this.name);
		if(this.property){
			return;
		}
		else{
			sw.append('(');
			ExpressionFactory.toEPLParameterList(this.paramList, sw);
			sw.append(')');
		}
	}
	*/
	
	/**
	public void toStringBuilderWithNickName(StringBuilder sw) {
		sw.append(this.name);
		if(this.property){
			return;
		}
		else{
			sw.append('(');
			//ExpressionFactory.toEPLParameterList(this.parameterList, sw);
			String delimiter="";
			for(AbstractResultExpression param: paramList){
				sw.append(delimiter);
				delimiter=",";
				param.toStringBuilderWithNickName(sw);
			}
			sw.append(')');
		}
	}
	*/
	
	/**
	@Override
	public int eigenCode() {
		int code=name.hashCode();//FIXME: name?
		if(this.property){
			return code;
		}
		else{
			for(AbstractPropertyExpression expr: paramList){
				code ^= expr.eigenCode();
			}
			return code;
		}
	}
	*/
	
	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		for(int i=0;i<paramList.size();i++){
			if(!paramList.get(i).resolve(ssw, null)){
				AbstractResultExpression newExpr=(AbstractResultExpression)ExpressionFactory.resolve(paramList.get(i),ssw,null);
				paramList.set(i, newExpr);
			}
		}
		return false;
	}
	
	/**
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		for(AbstractPropertyExpression param: paramList){
			param.dumpAllEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpAllEventOrPropertySpecReferences(
			Set<EventOrPropertySpecification> epsSet) {
		for(AbstractResultExpression param: paramList){
			param.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	*/
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitUDFDotExpressionItem(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitUDFDotExpressionItem(this, obj);
	}
}
