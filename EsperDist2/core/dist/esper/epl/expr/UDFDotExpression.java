package dist.esper.epl.expr;


import java.util.ArrayList;

import com.espertech.esper.client.soda.DotExpression;
import com.espertech.esper.client.soda.DotExpressionItem;
import com.espertech.esper.epl.expression.ExprChainedSpec;
import com.espertech.esper.epl.expression.ExprDotNode;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.UDEJsonSerializer.class)
public class UDFDotExpression extends AbstractResultExpression {
	private static final long serialVersionUID = -7171397188756022804L;
	ArrayList<UDFDotExpressionItem> itemList=new ArrayList<UDFDotExpressionItem>(2);
	
	public UDFDotExpression(){
	}
	
	public ArrayList<UDFDotExpressionItem> getItemList() {
		return itemList;
	}

	public void setItemList(ArrayList<UDFDotExpressionItem> chainSegList) {
		this.itemList = chainSegList;
	}

	public void addItem(UDFDotExpressionItem item){
		itemList.add(item);
	}

	public static class Factory{
		public static UDFDotExpression make(ExprDotNode edn){
			UDFDotExpression ue=new UDFDotExpression();
			for(ExprChainedSpec cs: edn.getChainSpec()){
				UDFDotExpressionItem ucs=UDFDotExpressionItem.Factory.make(cs);
				ue.addItem(ucs);
			}
			return ue;
		}
		
		public static UDFDotExpression make(DotExpression de0){
			UDFDotExpression de1=new UDFDotExpression();
			for(DotExpressionItem item0: de0.getChain()){
				UDFDotExpressionItem item1=UDFDotExpressionItem.Factory.make(item0);
				de1.addItem(item1);
			}
			return de1;
		}
	}
	
	/*
	@Override
	public void toStringBuilder(StringBuilder sw) {
		String delimiter="";
		for(UDFDotExpressionItem ucs: itemList){
			sw.append(delimiter);
			ucs.toStringBuilder(sw);
			delimiter=".";
		}
	}
	*/
	
	/**
	@Override
	public void toStringBuilderWithNickName(StringBuilder sw) {
		String delimiter="";
		for(UDFDotExpressionItem ucs: itemList){
			sw.append(delimiter);
			ucs.toStringBuilderWithNickName(sw);
			delimiter=".";
		}
	}
	*/
	
	/**
	public int eigenCode() {
		int code=0;
		for(UDFDotExpressionItem udfcs: itemList){
			code ^= udfcs.eigenCode();
		}
		return code;
	}
	*/

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		for(UDFDotExpressionItem ucs: itemList){
			ucs.resolve(ssw,null);
		}
		return true;
	}

	/**
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		for(UDFDotExpressionItem ucs: itemList){
			ucs.dumpAllEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		for(UDFDotExpressionItem ucs: itemList){
			ucs.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	*/
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitUDFDotExpression(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitUDFDotExpression(this, obj);
	}
}
