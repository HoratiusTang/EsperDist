package dist.esper.epl.expr;


import java.util.ArrayList;

import com.espertech.esper.client.soda.SelectClauseExpression;
import com.espertech.esper.epl.spec.*;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.sementic.IResolvable;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class SelectClause  extends AbstractClause{
	public ArrayList<SelectClauseElement> elementList=new ArrayList<SelectClauseElement>();
	public boolean distinct=false;
	public boolean hasWildcard=false;
	
	public SelectClause(){
		type="select";
    }

	public boolean isDistinct() {
		return distinct;
	}

	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}
	
	public boolean isHasWildcard() {
		return hasWildcard;
	}

	public void setHasWildcard(boolean hasWildcard) {
		this.hasWildcard = hasWildcard;
	}

	public ArrayList<SelectClauseElement> getElementList() {
		return elementList;
	}
	
	public void addElement(SelectClauseElement element){
		elementList.add(element);
	}
	
	public static class Factory{
		public static SelectClause make(SelectClauseSpecCompiled scs){
			SelectClause sc=new SelectClause();
			sc.setDistinct(scs.isDistinct());
			for(SelectClauseElementCompiled sce: scs.getSelectExprList()){
				if(sce instanceof SelectClauseElementWildcard){
					sc.addElement(SelectClauseWildcardElement.getInstance());
					sc.setHasWildcard(true);
				}
				else{
					sc.addElement(SelectClauseExpressionElement.Factory.make((SelectClauseExprCompiledSpec)sce));
				}
			}
			return sc;
		}
		
		public static SelectClause make(com.espertech.esper.client.soda.SelectClause sc0){
			SelectClause sc1=new SelectClause();
			sc1.setDistinct(sc0.isDistinct());
			for(com.espertech.esper.client.soda.SelectClauseElement sce0: sc0.getSelectList()){
				if(sce0 instanceof SelectClauseElementWildcard){
					sc1.addElement(SelectClauseWildcardElement.getInstance());
					sc1.setHasWildcard(true);
				}
				else if(sce0 instanceof com.espertech.esper.client.soda.SelectClauseExpression){
					SelectClauseExpressionElement sce1=SelectClauseExpressionElement.Factory.make((SelectClauseExpression)sce0);
					sc1.addElement(sce1);
				}
				else{//TODO: SelectClauseStreamWildcard: select streamOne.* from ...					
				}
			}
			return sc1;
		}
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(type);
		if(this.distinct){
			sw.append(" distinct");
		}
		String delimitor=" ";
		for(SelectClauseElement sce: elementList){
			sw.append(delimitor);
			sce.toStringBuilder(sw);
			delimitor=", ";
		}
	}
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw);
		return sw.toString();
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		for(SelectClauseElement sce: elementList){
			sce.resolve(ssw, param);
		}
		return true;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitSelectClause(this);
	}
}
