package dist.esper.epl.sementic;

import java.util.HashMap;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.PatternFilterNode;
import dist.esper.epl.expr.pattern.PatternMultiChildNode;
import dist.esper.epl.expr.pattern.PatternSingleChildNode;
import dist.esper.epl.expr.util.*;
import dist.esper.event.*;
import dist.esper.event.EventRegistry;

public class StatementVisitor 
implements IClauseVisitor<AbstractClause>, IExpressionVisitor2<AbstractExpression, Object> {
	long eplId=-1;
	HashMap<String,EventAlias> eventAliasMap=new HashMap<String,EventAlias>(8);
	//HashMap<String,EventPropertySpecification> propAliasMap=new HashMap<String,EventPropertySpecification>(8);
	//HashMap<String,EventAlias> noAliasFilterEventAliasNameMap=new HashMap<String,EventAlias>(8);
	EventRegistry eventRegistry=null;
	
	public StatementVisitor(long eplId, EventRegistry eventRegistry) {
		super();
		this.eplId = eplId;
		this.eventRegistry = eventRegistry;
	}
	
	public void visit(AbstractClause clause){
		clause.setEplId(eplId);
	}
	public void visit(AbstractExpression expr){
		expr.setEplId(eplId);
	}
	
	public AbstractExpression resolve(AbstractExpression expr, Object obj){
		UnsolvedEventOrPropertyExpression2 uep=(UnsolvedEventOrPropertyExpression2)expr;
		if(uep.isFullName()){
			EventAlias ea=this.eventAliasMap.get(uep.eventAsName);
			if(ea==null){
				throw new RuntimeException(String.format("can not find event with name %s", uep.eventAsName));
			}
			EventSpecification es=null;
			EventPropertySpecification eps=null;
			if(uep.eventIndex>=0)
				es=new EventIndexedSpecification(ea, uep.eventIndex, ea);//FIXME?
			else
				es=new EventSpecification(ea, ea);
			
			EventProperty prop=ea.getEvent().getProperty(uep.propName);
			if(uep.propIndex>=0)
				eps=new EventPropertyIndexedSpecification(es, prop, uep.propIndex);
			else
				eps=new EventPropertySpecification(es, prop);
			return eps;
		}
		else{
			if(obj instanceof EventAlias){
				EventAlias ea=(EventAlias)obj;
				EventProperty prop=ea.getEvent().getProperty(uep.eventAsNameOrPropName);
				if(prop!=null){
					EventSpecification es=new EventSpecification(ea);
					EventPropertySpecification eps=null;
					if(uep.eventIndexOrPropIndex>=0)
						eps=new EventPropertyIndexedSpecification(es, prop, uep.eventIndexOrPropIndex);					
					else
						eps=new EventPropertySpecification(es, prop);
					return eps;
				}
			}
			//treat as event's name
			EventAlias ea=this.eventAliasMap.get(uep.eventAsNameOrPropName);
			if(ea==null){
				throw new RuntimeException(String.format("can not find event with name %s", uep.eventAsName));
			}
			EventSpecification es=null;
			if(uep.eventIndexOrPropIndex>=0)
				es=new EventIndexedSpecification(ea, uep.eventIndexOrPropIndex, ea);//FIXME?			
			else
				es=new EventSpecification(ea, ea);			
			return es;
		}
	}
	
	@Override
	public AbstractExpression visitComparisionExpression(ComparisonExpression ce, Object obj) {
		visit(ce);
		for(int i=0;i<ce.getChildExprList().size();i++){
			AbstractResultExpression child=ce.getChildExprList().get(i);
			if(child.accept(this, obj)==null){
				AbstractResultExpression newExpr=(AbstractResultExpression)resolve(child, obj);
				//TODO: newExpr=(AbstractResultExpression)ExpressionFactory.resolve(childExprList.get(i),ssw, param);
				ce.getChildExprList().set(i, newExpr);
			}
		}
		AbstractResultExpression first=ce.getChildExprList().get(0);
		AbstractResultExpression second=ce.getChildExprList().get(1);
		//make EventOrPropertySpecification prior to Value
		if(first.getClass().getSimpleName().compareTo(second.getClass().getSimpleName())>0){
			ce.reverse();
		}
		return ce;
	}

	@Override
	public AbstractExpression visitCompositeExpression(CompositeExpression ce, Object obj) {
		visit(ce);
		for(AbstractBooleanExpression childExpr: ce.getChildExprList()){
			childExpr.accept(this, obj);
		}
		return ce;
	}

	@Override
	public AbstractExpression visitEventSpecification(EventSpecification es, Object obj) {
		visit(es);
		return es;
	}

	@Override
	public AbstractExpression visitEventIndexedSpecification(
			EventIndexedSpecification eis, Object obj) {
		visit(eis);
		return eis;
	}

	@Override
	public AbstractExpression visitEventPropertySpecification(
			EventPropertySpecification eps, Object obj) {
		visit(eps);
		return eps;
	}

	@Override
	public AbstractExpression visitEventPropertyIndexedSpecification(
			EventPropertyIndexedSpecification epis, Object obj) {
		visit(epis);
		return epis;
	}

	@Override
	public AbstractExpression visitTimePeriod(TimePeriod t, Object obj) {
		visit(t);
		return t;
	}

	@Override
	public AbstractExpression visitValue(Value v, Object obj) {
		visit(v);
		return v;
	}

	@Override
	public AbstractExpression visitAggregationExpression(
			AggregationExpression ae, Object obj) {
		visit(ae);
		if(ae.getExpr().accept(this, ae)==null){
			AbstractResultExpression newExpr=(AbstractResultExpression)resolve(ae.getExpr(), obj);
			//TODO
			ae.setExpr(newExpr);
		}
		if(ae.getFilterExpr()!=null){
			ae.getFilterExpr().accept(this, ae);
		}
		return ae;
	}

	@Override
	public AbstractExpression visitMathExpression(MathExpression me, Object obj) {
		visit(me);
		for(int i=0;i<me.getChildExprList().size();i++){
			AbstractResultExpression child=me.getChildExprList().get(i);
			if(child.accept(this, obj)==null){
				AbstractResultExpression newExpr=(AbstractResultExpression)resolve(child, obj);
				//TODO newExpr=(AbstractResultExpression)ExpressionFactory.resolve(childExprList.get(i),ssw,param);
				me.getChildExprList().set(i, newExpr);
			}
		}
		return me;
	}

	@Override
	public AbstractExpression visitUDFDotExpressionItem(UDFDotExpressionItem ucs, Object obj) {
		visit(ucs);
		for(int i=0;i<ucs.getParamList().size();i++){
			AbstractResultExpression child=ucs.getParamList().get(i);
			if(child.accept(this, ucs)==null){
				AbstractResultExpression newExpr=(AbstractResultExpression)resolve(child, obj);
				//TODO
				ucs.getParamList().set(i, newExpr);
			}
		}
		return ucs;
	}

	@Override
	public AbstractExpression visitUDFDotExpression(UDFDotExpression ue, Object obj) {
		visit(ue);
		for(UDFDotExpressionItem ucs: ue.getItemList()){
			ucs.accept(this, ue);
		}
		return ue;
	}

	@Override
	public AbstractExpression visitPattenMultiChildNode(
			PatternMultiChildNode pmcn, Object obj) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public AbstractExpression visitPattenFilterNode(PatternFilterNode pncn, Object obj) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public AbstractExpression visitPattenSingleChildNode(
			PatternSingleChildNode pscn, Object obj) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public AbstractClause visitBooleanExpressionClause(
			BooleanExpressionClause bec) {
		visit(bec);
		bec.getExpr().accept(this, bec);
		return bec;
	}

	@Override
	public AbstractClause visitFromClause(FromClause fc) {
		visit(fc);
		for(StreamSpecification streamSpec: fc.getStreamSpecList()){
			streamSpec.accept(this);
		}
		return fc;
	}

	@Override
	public AbstractClause visitGroupByClause(GroupByClause gbc) {
		visit(gbc);
		for(int i=0;i<gbc.getGroupByExprList().size();i++){
			AbstractIdentExpression child=gbc.getGroupByExprList().get(i);
			if(child.accept(this, gbc)==null){
				AbstractIdentExpression newExpr=(AbstractIdentExpression)resolve(child, null); 
				//TODO
				gbc.getGroupByExprList().set(i, newExpr);
			}
		}
		return gbc;
	}

	@Override
	public AbstractClause visitOrderByClause(OrderByClause obc) {
		visit(obc);
		for(OrderByElement obi: obc.getElementList()){
			obi.accept(this);
		}
		return obc;
	}

	@Override
	public AbstractClause visitOrderByElement(OrderByElement obe) {
		visit(obe);
		if(obe.getExpr().accept(this, obe)==null){
			AbstractIdentExpression newExpr=(AbstractIdentExpression)resolve(obe.getExpr(), null);
			//TODO
			obe.setExpr(newExpr);
		}
		return obe;
	}

	@Override
	public AbstractClause visitRowLimitClause(RowLimitClause rlc) {
		visit(rlc);
		return rlc;
	}

	@Override
	public AbstractClause visitSelectClause(SelectClause sc) {
		visit(sc);
		for(SelectClauseElement sce: sc.getElementList()){
			sce.accept(this);
		}
		return sc;
	}

	@Override
	public AbstractClause visitSelectClauseExpressionElement(
			SelectClauseExpressionElement sce) {
		visit(sce);
		if(sce.getSelectExpr().accept(this, sce)==null){
			AbstractResultExpression newExpr=(AbstractResultExpression)resolve(sce.getSelectExpr(), null);
			//TODO
			sce.setSelectExpr(newExpr);
		}
		return sce;
	}

	@Override
	public AbstractClause visitSelectClauseWildcardElement(
			SelectClauseWildcardElement sce) {
		return sce;
	}

	@Override
	public AbstractClause visitStatementSpecification(StatementSpecification ss) {
		visit(ss);
		ss.getFromClause().accept(this);
		ss.getSelectClause().accept(this);
		if(ss.getWhereClause()!=null){
			ss.getWhereClause().accept(this);
		}
		if(ss.getGroupByClause()!=null){
			ss.getGroupByClause().accept(this);
		}
		if(ss.getHavingClause()!=null){
			ss.getHavingClause().accept(this);
		}
		if(ss.getOrderByClause()!=null){
			ss.getOrderByClause().accept(this);
		}
		return ss;
	}

	@Override
	public AbstractClause visitFilterStreamSpecification(
			FilterStreamSpecification fss) {
		Event event=this.eventRegistry.resolveEvent(fss.getEventTypeName());
		EventAlias eventAlias=new EventAlias(eplId, event, fss);
		fss.setEventAlias(eventAlias);
		
		if(fss.getOptionalStreamName()!=null){
			eventAlias.setEventAsName(fss.getOptionalStreamName());
			this.eventAliasMap.put(fss.getOptionalStreamName(), eventAlias);
		}
		if(fss.getFilterExpr()!=null){
			fss.getFilterExpr().accept(this, eventAlias);
		}
		return fss;
	}

	@Override
	public AbstractClause visitPatternStreamSpecification(
			PatternStreamSpecification pss) {
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public AbstractExpression visitWildcardExpression(WildcardExpression we,
			Object obj) {
		return we;
	}

}
