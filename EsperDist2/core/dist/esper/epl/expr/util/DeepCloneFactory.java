package dist.esper.epl.expr.util;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;

public class DeepCloneFactory implements IExpressionVisitor<AbstractExpression> {
	
	static DeepCloneFactory instance=new DeepCloneFactory();
	
//	public SelectClauseExpressionElement deepClone(SelectClauseExpressionElement se){
//		AbstractResultExpression expr2=(AbstractResultExpression)deepClone(se.getSelectExpr());
//		SelectClauseExpressionElement se2=new SelectClauseExpressionElement(expr2);
//		se2.setAssigndName(se.getAssigndName());
//		se2.setUniqueName(se.getUniqueName());
//		return se2;
//	}
	
	public static AbstractExpression staticDeepClone(AbstractExpression ae){
		return ae.accept(instance);
	}
	
	public AbstractExpression deepClone(AbstractExpression ae){
		if(ae==null){
			return null;
		}
		return ae.accept(this);
	}
	
	@Override
	public AbstractExpression visitComparisionExpression(ComparisonExpression ce) {
		ComparisonExpression ce2=new ComparisonExpression(ce.getRelation());
		for(AbstractResultExpression child: ce.getChildExprList()){
			ce2.addExpression((AbstractResultExpression)deepClone(child));
		}
		return ce2;
	}

	@Override
	public AbstractExpression visitCompositeExpression(CompositeExpression ce) {
		CompositeExpression ce2=new CompositeExpression(ce.getRelation());
		for(AbstractBooleanExpression child: ce.getChildExprList()){
			ce2.addExpression((AbstractBooleanExpression)deepClone(child));
		}
		return ce2;
	}

	@Override
	public AbstractExpression visitEventSpecification(EventSpecification es) {
		EventSpecification es2=new EventSpecification(es.getEventAlias(), es.getOwnEventAlias());
		es2.setArray(es.isArray());
		return es2;
	}

	@Override
	public AbstractExpression visitEventIndexedSpecification(
			EventIndexedSpecification eis) {
		EventIndexedSpecification eis2=new EventIndexedSpecification(
				eis.getEventAlias(), eis.getIndex(), eis.getOwnEventAlias());
		return eis2;
	}

	@Override
	public AbstractExpression visitEventPropertySpecification(
			EventPropertySpecification eps) {
		EventPropertySpecification eps2=new EventPropertySpecification(
				(EventSpecification)deepClone(eps.getEventSpec()), 
				eps.getEventProp());
		eps2.setArray(eps.isArray());
		return eps2;
	}

	@Override
	public AbstractExpression visitEventPropertyIndexedSpecification(
			EventPropertyIndexedSpecification epis) {
		EventPropertyIndexedSpecification epis2=new EventPropertyIndexedSpecification(
				(EventSpecification)deepClone(epis.getEventSpec()), 
				epis.getEventProp(), 
				epis.getIndex());
		return epis2;
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
		AggregationExpression ae2=new AggregationExpression(ae.getAggType());
		ae2.setExpr((AbstractResultExpression)deepClone(ae.getExpr()));
		ae2.setFilterExpr((AbstractBooleanExpression)deepClone(ae.getFilterExpr()));
		ae2.setDistinct(ae.isDistinct());
		return ae2;
	}

	@Override
	public AbstractExpression visitMathExpression(MathExpression me) {
		MathExpression me2=new MathExpression(me.getOperationType());
		for(AbstractResultExpression child: me.getChildExprList()){
			me2.addExpression((AbstractResultExpression)deepClone(child));
		}
		return me2;
	}
	
	@Override
	public AbstractExpression visitUDFDotExpressionItem(UDFDotExpressionItem ucs) {
		UDFDotExpressionItem ucs2=new UDFDotExpressionItem(ucs.getName());
		ucs2.setProperty(ucs.isProperty());
		for(AbstractResultExpression param: ucs.getParamList()){
			ucs2.addParameter((AbstractResultExpression)deepClone(param));
		}
		return ucs2;
	}

	@Override
	public AbstractExpression visitUDFDotExpression(UDFDotExpression ue) {
		UDFDotExpression ue2=new UDFDotExpression();
		for(UDFDotExpressionItem ucs: ue.getItemList()){
			ue2.addItem((UDFDotExpressionItem)deepClone(ucs));
		}
		return ue2;
	}

	@Override
	public AbstractExpression visitPattenMultiChildNode(
			PatternMultiChildNode pmcn) {
		try {
			PatternMultiChildNode pmcn2=pmcn.getClass().newInstance();
			for(AbstractPatternNode child: pmcn.getChildNodeList()){
				pmcn2.addChildNode((AbstractPatternNode)deepClone(child));
			}
			return pmcn2;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public AbstractExpression visitPattenFilterNode(PatternFilterNode pncn) {
		PatternFilterNode pncn2=new PatternFilterNode();
		pncn2.setEventSpec((EventSpecification)deepClone(pncn.getEventSpec()));
		if(pncn.getFilterExpression()!=null){
			pncn2.setFilterExpression((AbstractBooleanExpression)deepClone(pncn.getFilterExpression()));
		}
		return pncn2;
	}

	@Override
	public AbstractExpression visitPattenSingleChildNode(
			PatternSingleChildNode pscn) {
		try {
			PatternSingleChildNode pscn2=pscn.getClass().newInstance();
			pscn2.setChildNode((AbstractPatternNode)deepClone(pscn.getChildNode()));
			return pscn2;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public AbstractExpression visitWildcardExpression(WildcardExpression we) {
		return we;
	}
}
