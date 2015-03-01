package dist.esper.epl.expr;



import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.event.EventProperty;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.EPISJsonSerializer.class)
public class EventPropertyIndexedSpecification extends EventPropertySpecification{
	private static final long serialVersionUID = 7626334598386155959L;
	public int index=-1;
	
	public EventPropertyIndexedSpecification() {
		super();
	}

	public EventPropertyIndexedSpecification(EventSpecification eventSpec,
			EventProperty eventProp, int index) {
		super(eventSpec, eventProp);
		this.index=index;
	}
	
	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}
	
	/**
	@Override
	public void toStringBuilder(StringBuilder sw){
		super.toStringBuilder(sw);
		sw.append("["+index+"]");
	}
	*/
	
	/**
	@Override
	public int eigenCode(){
		return super.eigenCode() ^ index;
	}
	*/
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof EventPropertyIndexedSpecification){
			EventPropertyIndexedSpecification eps=(EventPropertyIndexedSpecification)obj;
			if(eps.getEventAlias().equals(this.getEventAlias()) &&
					eps.getEventProp().equals(this.getEventProp()) &&
					eps.getIndex()==this.getIndex()){
				return true;
			}
		}
		return false;
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitEventPropertyIndexedSpecification(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitEventPropertyIndexedSpecification(this, obj);
	}
	
//	@Override
//	public int getRelation(EventOrPropertySpecification eps) {
//		if(eps instanceof EventPropertyIndexedSpecification){
//			EventPropertyIndexedSpecification pis=(EventPropertyIndexedSpecification)eps;
//			if(this.eventSpec.getEventAlias().equals(pis.getEventSpecification().getEventAlias()) && 
//					this.eventProp.equals(pis.getEventProperty()) &&
//					this.index==pis.getIndex()){
//					return EventOrPropertySpecification.Relation.EQUAL;
//			}
//		}
//		else if(eps instanceof EventPropertySpecification){
//			EventPropertySpecification ps=(EventPropertySpecification)eps;
//			if(this.eventSpec.getEventAlias().equals(ps.getEventSpecification().getEventAlias())){
//				if(this.eventProp.equals(ps.getEventProperty())){
//					return EventOrPropertySpecification.Relation.IS_CONTAINED;
//				}
//			}
//		}
//		else if(eps instanceof EventSpecification){
//			EventSpecification es=(EventSpecification)eps;
//			if(this.eventSpec.getEventAlias().equals(es.getEventAlias())){
//				return EventOrPropertySpecification.Relation.IS_CONTAINED;
//			}
//		}		
//		return EventOrPropertySpecification.Relation.NONE;
//	}
}
