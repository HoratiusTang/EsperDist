package dist.esper.epl.expr;



import com.espertech.esper.client.soda.*;
import com.espertech.esper.epl.spec.FilterStreamSpecCompiled;
import com.espertech.esper.epl.spec.PatternStreamSpecCompiled;
import com.espertech.esper.filter.FilterSpecCompiled;
import com.espertech.esper.filter.FilterSpecParam;
import com.espertech.esper.filter.FilterSpecParamExprNode;
import com.espertech.esper.pattern.EvalFilterFactoryNode;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.event.Event;

public class FilterStreamSpecification extends StreamSpecification {
	private FilterStreamSpecCompiled filterStreamSpec=null;
	private String eventTypeName=null;
	//private String streamName=null;
	AbstractBooleanExpression filterExpr=null;
	EventAlias eventAlias=null;
	
	public FilterStreamSpecification(){
		super();
	}
	public FilterStreamSpecification(FilterStreamSpecCompiled filterStreamSpec) {
		super();
		this.filterStreamSpec = filterStreamSpec;
	}
	
//	public String getOptionalStreamName(){
//		return filterStreamSpec.getOptionalStreamName();
//	}
	
	public String getEventTypeName(){
		//return filterStreamSpec.getFilterSpec().getFilterForEventTypeName();
		return eventTypeName;
	}	
	
	public void setEventTypeName(String eventTypeName) {
		this.eventTypeName = eventTypeName;
	}
	
//	public String getStreamName() {
//		return streamName;
//	}
//	public void setStreamName(String streamName) {
//		this.streamName = streamName;
//	}
	public AbstractBooleanExpression getFilterExpr() {
		return filterExpr;
	}

	public void setFilterExpr(AbstractBooleanExpression filterExpr) {
		this.filterExpr = filterExpr;
	}

	public EventAlias getEventAlias() {
		return eventAlias;
	}

	public void setEventAlias(EventAlias eventAlias) {
		this.eventAlias = eventAlias;
	}


	public static class Factory{
		public static FilterStreamSpecification make(FilterStreamSpecCompiled fssc){
			FilterStreamSpecification fss=new FilterStreamSpecification(fssc);
			fss.setEventTypeName(fss.filterStreamSpec.getFilterSpec().getFilterForEventTypeName());
			fss.setOptionalStreamName(fss.filterStreamSpec.getOptionalStreamName());		
			ViewSpecification[] vss=ViewSpecification.Factory.makeViewSpecs(fss.filterStreamSpec.getViewSpecs());
			fss.setViewSpecs(vss);
			FilterSpecCompiled fsc=fss.filterStreamSpec.getFilterSpec();
			FilterSpecParam[] fsps=fsc.getParameters();
			if(fsps!=null && fsps.length>0){
//				if(fsps[0] instanceof FilterSpecParamExprNode){
//					FilterSpecParamExprNode fspen=(FilterSpecParamExprNode)fsps[0];
//					fss.filterExpr=(IBooleanExpression)(ExpressionFactory.toExpression(fspen.getExprNode()));
//				}
				fss.filterExpr=FilterFactory.toBooleanExpression(fsps, fss.getOptionalStreamName());
			}
			return fss;
		}
		
		public static FilterStreamSpecification make(com.espertech.esper.client.soda.FilterStream fs0){
			FilterStreamSpecification fs1=new FilterStreamSpecification();
			AbstractBooleanExpression filterExpr=(AbstractBooleanExpression)ExpressionFactory1.toExpression1(fs0.getFilter().getFilter());
			ViewSpecification[] vs1=ViewSpecification.Factory.makeViewSpecs(fs0.getViews());
			
			fs1.setFilterExpr(filterExpr);
			fs1.setViewSpecs(vs1);
			fs1.setEventTypeName(fs0.getFilter().getEventTypeName());
			fs1.setOptionalStreamName(fs0.getStreamName());
			
			return fs1;
		}
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(getEventTypeName());
		if(filterExpr!=null){
			filterExpr.toStringBuilder(sw);
		}
		if(this.viewSpecs!=null && this.viewSpecs.length>0){
			for(ViewSpecification vs: viewSpecs){
				sw.append(".");
				vs.toStringBuilder(sw);
			}
		}
		if(getOptionalStreamName()!=null){
			sw.append(" as ");
			sw.append(getOptionalStreamName());
		}
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		//solve EventAlias
		Event event=ssw.eventRegistry.resolveEvent(getEventTypeName());
		assert(event!=null):getEventTypeName();
		eventAlias=new EventAlias(ssw.eplId, event,this);
		
		if(getOptionalStreamName()!=null){
			eventAlias.setEventAsName(getOptionalStreamName());
			ssw.eventAliasMap.put(getOptionalStreamName(), eventAlias);
		}
		//else{
			//ssw.noAliasFilterEventAliasFullNameMap.put(eventAlias.getEvent().getName(), eventAlias);
			ssw.noAliasFilterEventAliasNameMap.put(eventAlias.getEvent().getName(), eventAlias);
		//}
		if(filterExpr!=null){
			filterExpr.resolve(ssw,eventAlias);
		}
		return true;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitFilterStreamSpecification(this);
	}
}
