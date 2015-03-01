package dist.esper.epl.expr;


import java.util.ArrayList;

import com.espertech.esper.epl.spec.OrderByItem;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.expr.util.Stringlizable;
import dist.esper.epl.sementic.IResolvable;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class OrderByClause extends AbstractClause{
	ArrayList<OrderByElement> elementList=new ArrayList<OrderByElement>(2);
	
	public OrderByClause(){
		this.type="order by";
	}
	
	public ArrayList<OrderByElement> getElementList(){
		return elementList;
	}

	public void addElement(OrderByElement obe){
		elementList.add(obe);
	}
	
	public static class Factory{
		public static OrderByClause make(OrderByItem[] items){
			if(items==null || items.length==0){
				return null;
			}
			OrderByClause obc=new OrderByClause();
			for(OrderByItem item: items){
				AbstractIdentExpression expr=(AbstractIdentExpression)ExpressionFactory.toExpression(item.getExprNode());
				obc.addElement(new OrderByElement(expr,item.isDescending()));
			}
			return obc;
		}
		
		public static OrderByClause make(com.espertech.esper.client.soda.OrderByClause obc0){
			OrderByClause obc1=new OrderByClause();
			for(com.espertech.esper.client.soda.OrderByElement obe0: obc0.getOrderByExpressions()){
				AbstractIdentExpression expr1=(AbstractIdentExpression)ExpressionFactory1.toExpression1(obe0.getExpression());
				obc1.addElement(new OrderByElement(expr1, obe0.isDescending()));
			}
			return obc1;
		}
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(type);
		String delimitor=" ";
		for(OrderByElement obi: elementList){
			sw.append(delimitor);
			obi.toStringBuilder(sw);
			delimitor=", ";
		}
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		for(OrderByElement obi: elementList){
			obi.resolve(ssw, param);
		}
		return false;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitOrderByClause(this);
	}
}
