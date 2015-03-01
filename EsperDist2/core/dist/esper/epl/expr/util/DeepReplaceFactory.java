package dist.esper.epl.expr.util;

import java.util.Map;

import dist.esper.core.flow.container.SelectExpressionElementContainer;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;

public class DeepReplaceFactory implements IExpressionVisitor<AbstractExpression> {
	Map<EventAlias,EventAlias> eaMap;
//	static DeepReplaceFactory instance=new DeepReplaceFactory();
	
	public DeepReplaceFactory(){
	}
	public DeepReplaceFactory(Map<EventAlias, EventAlias> eaMap) {
		super();
		this.eaMap = eaMap;
	}
	
	public Map<EventAlias, EventAlias> getEventAliasReplaceMap() {
		return eaMap;
	}

	public void setEventAliasReplaceMap(Map<EventAlias, EventAlias> eaMap) {
		this.eaMap = eaMap;
	}

	public AbstractExpression deepReplace(AbstractExpression ae){
		if(ae==null){
			return null;
		}
		return ae.accept(this);
	}
	
	public SelectExpressionElementContainer deepReplace(SelectExpressionElementContainer se){
		deepReplace(se.getSelectExpr());
		return se;
	}
	
	public SelectClauseExpressionElement deepReplace(SelectClauseExpressionElement se){
		deepReplace(se.getSelectExpr());
		return se;
	}
	
	@Override
	public String toString(){
		return String.format("%s:%s", this.getClass().getSimpleName(), eaMap.toString());
	}

	@Override
	public AbstractExpression visitComparisionExpression(ComparisonExpression ce) {
		for(AbstractResultExpression child: ce.getChildExprList()){
			deepReplace(child);
		}
		return ce;
	}

	@Override
	public AbstractExpression visitCompositeExpression(CompositeExpression ce) {
		for(AbstractBooleanExpression child: ce.getChildExprList()){
			deepReplace(child);
		}
		return ce;
	}

	@Override
	public AbstractExpression visitEventSpecification(EventSpecification es) {
		EventAlias e1=eaMap.get(es.getEventAlias());
		EventAlias e2=eaMap.get(es.getOwnEventAlias());
		if(e1==null){
			System.out.print("");
		}
		assert(e1!=null);
		//assert(e2!=null);
		es.setEventAlias(e1);
		es.setOwnEventAlias(e2);
		return es;
	}

	@Override
	public AbstractExpression visitEventIndexedSpecification(
			EventIndexedSpecification eis) {
		EventAlias e1=eaMap.get(eis.getEventAlias());
		EventAlias e2=eaMap.get(eis.getOwnEventAlias());
		assert(e1!=null);
		assert(e2!=null);
		eis.setEventAlias(e1);
		eis.setOwnEventAlias(e2);
		return eis;
	}

	@Override
	public AbstractExpression visitEventPropertySpecification(
			EventPropertySpecification eps) {
		deepReplace(eps.getEventSpec());
		return eps;
	}

	@Override
	public AbstractExpression visitEventPropertyIndexedSpecification(
			EventPropertyIndexedSpecification epis) {
		deepReplace(epis.getEventSpec());
		return epis;
	}

	@Override
	public AbstractExpression visitTimePeriod(TimePeriod t) {
		return t;
	}

	@Override
	public AbstractExpression visitValue(Value v) {
		return v;
	}

	@Override
	public AbstractExpression visitAggregationExpression(
			AggregationExpression ae) {
		deepReplace(ae.getExpr());
		deepReplace(ae.getFilterExpr());
		return ae;
	}

	@Override
	public AbstractExpression visitMathExpression(MathExpression me) {
		for(AbstractResultExpression child: me.getChildExprList()){
			deepReplace(child);
		}
		return me;
	}
	
	@Override
	public AbstractExpression visitUDFDotExpressionItem(UDFDotExpressionItem ucs) {
		for(AbstractResultExpression param: ucs.getParamList()){
			deepReplace(param);
		}
		return ucs;
	}

	@Override
	public AbstractExpression visitUDFDotExpression(UDFDotExpression ue) {
		for(UDFDotExpressionItem ucs: ue.getItemList()){
			deepReplace(ucs);
		}
		return ue;
	}

	@Override
	public AbstractExpression visitPattenMultiChildNode(
			PatternMultiChildNode pmcn) {			
		for(AbstractPatternNode child: pmcn.getChildNodeList()){
			deepReplace(child);
		}
		return pmcn;
	}

	@Override
	public AbstractExpression visitPattenFilterNode(PatternFilterNode pncn) {
		deepReplace(pncn.getEventSpec());
		if(pncn.getFilterExpression()!=null){
			deepReplace(pncn.getFilterExpression());
		}
		return pncn;
	}

	@Override
	public AbstractExpression visitPattenSingleChildNode(
			PatternSingleChildNode pscn) {
		deepReplace(pscn.getChildNode());
		return pscn;
		
	}

	@Override
	public AbstractExpression visitWildcardExpression(WildcardExpression we) {
		return we;
	}
}
