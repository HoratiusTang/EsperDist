package dist.esper.epl.expr;



import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.EISJsonSerializer.class)
public class EventIndexedSpecification extends EventSpecification{
	private static final long serialVersionUID = 8168932687763320055L;
	public int index=-1;

	public EventIndexedSpecification() {
		super();
	}

	public EventIndexedSpecification(EventAlias eventAlias, int index, EventAlias relatedEventAlias) {
		super(eventAlias, relatedEventAlias);
		this.index = index;
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
		if(obj instanceof EventIndexedSpecification){
			EventIndexedSpecification eis=(EventIndexedSpecification)obj;
			if(this.getEventAlias().equals(eis.getEventAlias()) &&
					this.getIndex()==eis.getIndex()){
				return true;
			}
		}
		return false;
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitEventIndexedSpecification(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitEventIndexedSpecification(this, obj);
	}
	
//	@Override
//	public int getRelation(EventOrPropertySpecification eps) {
//		if(eps instanceof EventSpecification){
//			EventSpecification es=(EventSpecification)eps;
//			if(es.getEventAlias().equals(this.getEventAlias())){
//				return EventOrPropertySpecification.Relation.IS_CONTAINED;
//			}
//		}
//		else if(eps instanceof EventIndexedSpecification){
//			EventIndexedSpecification eis=(EventIndexedSpecification)eps;
//			if(eis.getEventAlias().equals(this.getEventAlias()) &&
//					eis.getIndex() == this.getIndex()){
//				return EventOrPropertySpecification.Relation.EQUAL;
//			}
//		}
//		return EventOrPropertySpecification.Relation.NONE;
//	}
}
