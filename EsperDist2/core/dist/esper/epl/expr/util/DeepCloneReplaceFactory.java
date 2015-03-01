package dist.esper.epl.expr.util;

import java.util.Map;

import dist.esper.epl.expr.AbstractExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventIndexedSpecification;
import dist.esper.epl.expr.EventSpecification;

public class DeepCloneReplaceFactory extends DeepCloneFactory {
	Map<EventAlias,EventAlias> eaMap;

	public DeepCloneReplaceFactory(){
	}
	
	public DeepCloneReplaceFactory(Map<EventAlias, EventAlias> eaMap) {
		super();
		this.eaMap = eaMap;
	}
	
	public Map<EventAlias, EventAlias> getEventAliasReplaceMap() {
		return eaMap;
	}

	public void setEventAliasReplaceMap(Map<EventAlias, EventAlias> eaMap) {
		this.eaMap = eaMap;
	}
	
	@Override
	public AbstractExpression visitEventSpecification(EventSpecification es) {
		EventAlias e1=eaMap.get(es.getEventAlias());
		EventAlias e2=eaMap.get(es.getOwnEventAlias());
		assert(e1!=null);
		assert(e2!=null);
		EventSpecification es2=new EventSpecification(e1, e2);
		es2.setArray(es.isArray());
		return es2;
	}

	@Override
	public AbstractExpression visitEventIndexedSpecification(
			EventIndexedSpecification eis) {
		EventAlias e1=eaMap.get(eis.getEventAlias());
		EventAlias e2=eaMap.get(eis.getOwnEventAlias());
		assert(e1!=null);
		assert(e2!=null);
		EventIndexedSpecification eis2=new EventIndexedSpecification(
				e1, eis.getIndex(), e2);
		return eis2;
	}
}

