package dist.esper.epl.expr;


import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.event.EventProperty;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.EPSJsonSerializer.class)
public class EventPropertySpecification extends EventOrPropertySpecification{
	private static final long serialVersionUID = -9032081480369265409L;
	EventProperty eventProp=null;
	EventSpecification eventSpec=null;
	
	public EventPropertySpecification() {
		super();
	}

	public EventPropertySpecification(EventSpecification eventSpec, EventProperty eventProp) {
		super();
		this.eventProp = eventProp;
		this.eventSpec = eventSpec;
		this.setArray(eventProp.isArray());
	}
	
	public EventProperty getEventProp() {
		return eventProp;
	}

	public void setEventProp(EventProperty eventProp) {
		this.eventProp = eventProp;
	}

	public EventSpecification getEventSpec() {
		return eventSpec;
	}

	public void setEventSpec(EventSpecification eventAlias) {
		this.eventSpec = eventAlias;
	}
	
	@Override
	public EventAlias getEventAlias() {
		return this.eventSpec.getEventAlias();
	}

	/**
	@Override
	public void toStringBuilder(StringBuilder sw) {
		eventSpec.toStringBuilder(sw);
		sw.append(".");
		sw.append(eventProp.getName());
	}
	*/
	
	/**
	@Override
	public int eigenCode(){
		return eventProp.getName().hashCode() ^ eventSpec.eigenCode();
	}
	*/

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) {
		// TODO Auto-generated method stub
		return true;
	}

	/**
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		eventSpec.dumpAllEventAliases(eaSet);
	}
	*/
	
	@Override
	public boolean equals(Object obj){
		if(obj.getClass().getSimpleName().equals(this.getClass().getSimpleName())){
			EventPropertySpecification eps=(EventPropertySpecification)obj;
			if(eps.getEventAlias().equals(this.getEventAlias()) &&
					eps.getEventProp().equals(this.getEventProp())){
				return true;
			}
		}
		return false;
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitEventPropertySpecification(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitEventPropertySpecification(this, obj);
	}

//	@Override
//	public int getRelation(EventOrPropertySpecification eps) {
//		if(eps instanceof EventSpecification){
//			EventSpecification es=(EventSpecification)eps;
//			if(this.eventSpec.getEventAlias().equals(es.getEventAlias())){
//				return EventOrPropertySpecification.Relation.IS_CONTAINED;
//			}
//		}
//		else if(eps instanceof EventPropertySpecification){
//			EventPropertySpecification ps=(EventPropertySpecification)eps;
//			if(this.eventSpec.getEventAlias().equals(ps.getEventSpecification().getEventAlias())){
//				if(this.eventProp.equals(ps.getEventProperty())){
//					return EventOrPropertySpecification.Relation.EQUAL;
//				}
//			}
//		}
//		return EventOrPropertySpecification.Relation.NONE;
//	}
}
