package dist.esper.epl.expr;


import java.util.Set;

import com.espertech.esper.client.soda.SelectClauseExpression;
import com.espertech.esper.epl.spec.SelectClauseExprCompiledSpec;

import dist.esper.epl.expr.util.EventOrPropertySpecReferenceDumper;
import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.SelectElementJsonSerializer.class)
public class SelectClauseExpressionElement extends SelectClauseElement{
	private static final long serialVersionUID = 1399074763450078074L;
	String uniqueName=null;
	String assigndName=null;
	boolean events=false;
	AbstractResultExpression selectExpr=null;
	
	public SelectClauseExpressionElement(){
	}
	
	public SelectClauseExpressionElement(AbstractResultExpression selectExpr) {
		super();
		this.selectExpr = selectExpr;
	}
	
	public SelectClauseExpressionElement(AbstractResultExpression selectExpr, 
			String assigndName, boolean isEvents) {
		super();
		this.assigndName = assigndName;
		this.events = isEvents;
		this.selectExpr = selectExpr;
	}
	
	public String getAssigndName() {
		return assigndName;
	}

	public void setAssigndName(String assigndName) {
		this.assigndName = assigndName;
	}	

	public String getUniqueName() {
		return uniqueName;
	}

	public void setUniqueName(String uniqueName) {
		this.uniqueName = uniqueName;
	}

	public boolean isEvents() {
		return events;
	}
	
	public boolean getEvents() {
		return events;
	}

	public void setEvents(boolean isEvents) {
		this.events = isEvents;
	}

	public AbstractPropertyExpression getSelectExpr() {
		return selectExpr;
	}

	public void setSelectExpr(AbstractResultExpression selectExpr) {
		this.selectExpr = selectExpr;
	}

	public static class Factory{
		public static SelectClauseExpressionElement make(SelectClauseExprCompiledSpec sce){
			AbstractResultExpression selectExpr=(AbstractResultExpression)ExpressionFactory.toExpression(sce.getSelectExpression());
			SelectClauseExpressionElement scee=new SelectClauseExpressionElement(selectExpr);
			scee.setAssigndName(sce.getAssignedName());
			scee.setEvents(sce.isEvents());
			return scee;
		}
		
		public static SelectClauseExpressionElement make(SelectClauseExpression sce0){
			SelectClauseExpressionElement sce1=new SelectClauseExpressionElement();
			sce1.setAssigndName(sce0.getAsName());
			AbstractResultExpression selectExpr=(AbstractResultExpression)ExpressionFactory1.toExpression1(sce0.getExpression());
			sce1.setSelectExpr(selectExpr);
			return sce1;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		selectExpr.toStringBuilder(sw);
		if(this.assigndName!=null){
			sw.append(" as ");
			sw.append(this.assigndName);
		}
	}
	
	/**
	//@Override
	public void toStringBuilderWithNickName(StringBuilder sw){
		selectExpr.toStringBuilderWithNickName(sw);
	}
	*/
	
	/**
	public String toStringBuilderWithNickName(){
		StringBuilder sw=new StringBuilder();
		selectExpr.toStringBuilderWithNickName(sw);
		return sw.toString();
	}
	*/

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		if(!selectExpr.resolve(ssw, null)){
			selectExpr=(AbstractIdentExpression)ExpressionFactory.resolve(selectExpr,ssw,param);
		}
		return true;
	}
	
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet){
		//selectExpr.dumpAllEventOrPropertySpecReferences(epsSet);
		EventOrPropertySpecReferenceDumper.dump(selectExpr, epsSet);
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitSelectClauseExpressionElement(this);
	}
}
