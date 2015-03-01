package dist.esper.epl.expr;


import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.ESJsonSerializer.class)
public class EventSpecification extends EventOrPropertySpecification{
	private static final long serialVersionUID = -3957446236865753338L;
	public EventAlias ownEventAlias=null;
	public EventAlias eventAlias=null;

	public EventSpecification() {
		super();
	}

	public EventSpecification(EventSpecification eventSpec){
		super();
		this.eventAlias = eventSpec.eventAlias;
		this.ownEventAlias = eventSpec.ownEventAlias;
		this.array = eventSpec.array;
	}
	
	public EventSpecification(EventAlias eventAlias) {
		this(eventAlias,eventAlias);
	}
	
	public EventSpecification(EventAlias eventAlias, EventAlias ownEventAlias) {
		super();
		this.eventAlias = eventAlias;
		this.ownEventAlias = ownEventAlias;
	}

	@Override
	public EventAlias getEventAlias() {
		return eventAlias;
	}

	public void setEventAlias(EventAlias eventAlias) {
		this.eventAlias = eventAlias;
	}

	public EventAlias getOwnEventAlias() {
		return ownEventAlias;
	}

	public void setOwnEventAlias(EventAlias relatedEventAlias) {
		this.ownEventAlias = relatedEventAlias;
	}

	/**
	@Override
	public void toStringBuilder(StringBuilder sw) {
		eventAlias.toStringBuilder(sw);
	}
	*/
	
	/**
	@Override
	public int eigenCode(){
		return eventAlias.eigenCode();
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
		//if(eventAlias!=ownEventAlias){
			eaSet.add(eventAlias);
		//}
	}
	*/
	
	@Override
	public boolean equals(Object obj){
		if(obj.getClass().getSimpleName().equals(this.getClass().getSimpleName())){
			EventSpecification eis=(EventSpecification)obj;
			if(this.getEventAlias().equals(eis.getEventAlias())){
				return true;
			}
		}
		return false;
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitEventSpecification(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitEventSpecification(this, obj);
	}

//	@Override
//	public int getRelation(EventOrPropertySpecification eps) {
//		if(eps instanceof EventSpecification){
//			EventSpecification es=(EventSpecification)eps;
//			if(es.getEventAlias().equals(this.getEventAlias())){
//				return EventOrPropertySpecification.Relation.EQUAL;
//			}
//		}
//		return EventOrPropertySpecification.Relation.NONE;
//	}
	
}
