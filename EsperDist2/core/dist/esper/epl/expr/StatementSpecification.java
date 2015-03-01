package dist.esper.epl.expr;


import java.util.ArrayList;

import com.espertech.esper.client.soda.EPStatementObjectModel;
import com.espertech.esper.epl.spec.FilterStreamSpecCompiled;
import com.espertech.esper.epl.spec.PatternStreamSpecCompiled;
import com.espertech.esper.epl.spec.RowLimitSpec;
import com.espertech.esper.epl.spec.StatementSpecCompiled;
import com.espertech.esper.epl.spec.StreamSpecCompiled;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.expr.util.Stringlizable;
import dist.esper.epl.sementic.IResolvable;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.util.Logger2;

public class StatementSpecification extends AbstractClause{
	static Logger2 log=Logger2.getLogger(StatementSpecification.class);
	long eplId;
	SelectClause selectClause=null;	
	FromClause fromClause=null;
	BooleanExpressionClause whereClause=null;
	GroupByClause groupByClause=null;
	BooleanExpressionClause havingClause=null;
	OrderByClause orderByClause=null;
	RowLimitClause rowLimitSpec=null;
	
	public StatementSpecification(){
	}

	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}

	public SelectClause getSelectClause() {
		return selectClause;
	}
	
	public void setSelectClause(SelectClause selectClause) {
		this.selectClause = selectClause;
	}
	
	public FromClause getFromClause() {
		return fromClause;
	}

	public void setFromClause(FromClause fromClause) {
		this.fromClause = fromClause;
	}

	public BooleanExpressionClause getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(BooleanExpressionClause whereClause) {
		this.whereClause = whereClause;
	}

	public BooleanExpressionClause getHavingClause() {
		return havingClause;
	}

	public void setHavingClause(BooleanExpressionClause havingClause) {
		this.havingClause = havingClause;
	}

	public GroupByClause getGroupByClause() {
		return groupByClause;
	}

	public void setGroupByClause(GroupByClause groupByClause) {
		this.groupByClause = groupByClause;
	}
	
	public OrderByClause getOrderByClause() {
		return orderByClause;
	}
	
	public void setOrderByClause(OrderByClause orderByClause){
		this.orderByClause = orderByClause;
	}
	
	public RowLimitClause getRowLimitSpec() {
		return rowLimitSpec;
	}

	public void setRowLimitSpec(RowLimitClause rowLimitSpec) {
		this.rowLimitSpec = rowLimitSpec;
	}	
	
	public static class Factory{
		public static StatementSpecification make(StatementSpecCompiled ssc){
			StatementSpecification ss=new StatementSpecification();
			
			SelectClause selectClause=null;
			GroupByClause groupByClause=null;
			OrderByClause orderByClause=null;
			RowLimitClause rowLimitSpec=null;
			FromClause fromClause=null;
			BooleanExpressionClause whereClause=null;
			BooleanExpressionClause havingClause=null;
			
			log.info("-- patternStream or filterStream ---");
			fromClause=FromClause.Factory.make(ssc.getStreamSpecs());
			
			log.info("-- select ---");
			selectClause=SelectClause.Factory.make(ssc.getSelectClauseSpec());
			if(ssc.getFilterRootNode()!=null){
				log.info("-- filter ---");
				whereClause=BooleanExpressionClause.Factory.make(ssc.getFilterRootNode(), "where");
			}
			if(ssc.getGroupByExpressions()!=null){
				log.info("-- group by ---");
				groupByClause=GroupByClause.Factory.make(ssc.getGroupByExpressions());
			}
			if(ssc.getHavingExprRootNode()!=null){
				log.info("-- having ---");
				havingClause=BooleanExpressionClause.Factory.make(ssc.getHavingExprRootNode(), "having");
			}
			if(ssc.getOrderByList()!=null){
				log.info("-- order by ---");
				orderByClause=OrderByClause.Factory.make(ssc.getOrderByList());
			}
			if(ssc.getRowLimitSpec()!=null){				
				RowLimitSpec rls=ssc.getRowLimitSpec();
				rowLimitSpec=new RowLimitClause(rls.getNumRows().intValue(),
						rls.getOptionalOffset()==null?0:rls.getOptionalOffset().intValue());
			}
			ss.setSelectClause(selectClause);
			ss.setFromClause(fromClause);
			ss.setWhereClause(whereClause);
			ss.setGroupByClause(groupByClause);
			ss.setHavingClause(havingClause);
			ss.setOrderByClause(orderByClause);
			ss.setRowLimitSpec(rowLimitSpec);
			return ss;
		}
		
		public static StatementSpecification make(EPStatementObjectModel som){
			StatementSpecification ss=new StatementSpecification();
			
			SelectClause selectClause=null;
			GroupByClause groupByClause=null;
			OrderByClause orderByClause=null;
			RowLimitClause rowLimitSpec=null;
			FromClause fromClause=null;
			BooleanExpressionClause whereClause=null;
			BooleanExpressionClause havingClause=null;
			
			log.info("-- patternStream or filterStream ---");
			fromClause=FromClause.Factory.make(som.getFromClause());
			
			log.info("-- select ---");
			selectClause=SelectClause.Factory.make(som.getSelectClause());
			if(som.getWhereClause()!=null){
				log.info("-- where ---");
				whereClause=BooleanExpressionClause.Factory.make(som.getWhereClause(), "where");
			}
			if(som.getGroupByClause()!=null){
				log.info("-- group by ---");
				groupByClause=GroupByClause.Factory.make(som.getGroupByClause());
			}
			if(som.getHavingClause()!=null){
				log.info("-- having ---");
				havingClause=BooleanExpressionClause.Factory.make(som.getHavingClause(), "having");
			}
			if(som.getOrderByClause()!=null){
				log.info("-- order by ---");
				orderByClause=OrderByClause.Factory.make(som.getOrderByClause());
			}
			if(som.getRowLimitClause()!=null){				
				com.espertech.esper.client.soda.RowLimitClause rls=som.getRowLimitClause();
				rowLimitSpec=new RowLimitClause(rls.getNumRows().intValue(),
						rls.getOptionalOffsetRows()==null?0:rls.getOptionalOffsetRows().intValue());
			}
			ss.setSelectClause(selectClause);
			ss.setFromClause(fromClause);
			ss.setWhereClause(whereClause);
			ss.setGroupByClause(groupByClause);
			ss.setHavingClause(havingClause);
			ss.setOrderByClause(orderByClause);
			ss.setRowLimitSpec(rowLimitSpec);
			return ss;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		selectClause.toStringBuilder(sw);
		sw.append(" ");
		fromClause.toStringBuilder(sw);
		
		if(whereClause!=null){
			sw.append(" ");
			whereClause.toStringBuilder(sw);
		}
		if(groupByClause!=null){
			sw.append(" ");
			groupByClause.toStringBuilder(sw);
		}
		if(havingClause!=null){
			sw.append(" ");
			havingClause.toStringBuilder(sw);
		}
		if(orderByClause!=null){
			sw.append(" ");
			orderByClause.toStringBuilder(sw);
		}
		
		if(rowLimitSpec!=null){
			sw.append(" ");
			rowLimitSpec.toStringBuilder(sw);
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
		fromClause.resolve(ssw, param);
		selectClause.resolve(ssw, param);
		
		if(whereClause!=null){		
			whereClause.resolve(ssw, param);
		}
		if(groupByClause!=null){			
			groupByClause.resolve(ssw, param);
		}
		if(havingClause!=null){
			havingClause.resolve(ssw, param);
		}
		if(orderByClause!=null){
			orderByClause.resolve(ssw, param);
		}
		return true;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitStatementSpecification(this);
	}
}
