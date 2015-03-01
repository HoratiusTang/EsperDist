package dist.esper.epl.expr;


import java.util.Set;

public abstract class EventOrPropertySpecification extends AbstractIdentExpression{
	private static final long serialVersionUID = 2328976803454551330L;

	/*
	public static class Relation{
		public static int NONE=Integer.MAX_VALUE;
		public static int IS_CONTAINED=-1;
		public static int EQUAL=0;
		public static int CONTAINS=1;
		
		public static int reverse(int r){
			return (r==NONE)?NONE:EQUAL-r;
		}
	}
	*/
	boolean array=false;
	String internalEventNickName=null;
	String internalEventPropertyName=null;
	public String getInternalEventPropertyName() {
		return internalEventPropertyName;
	}

	public void setInternalEventPropertyName(String internalEventPropertyName) {
		this.internalEventPropertyName = internalEventPropertyName;
	}

	public String getInternalEventNickName() {
		return internalEventNickName;
	}

	public void setInternalEventNickName(String internalEventNickName) {
		this.internalEventNickName = internalEventNickName;
	}

	public boolean isArray() {
		return array;
	}
	
	public boolean getArray() {
		return array;
	}

	public void setArray(boolean isArray) {
		this.array = isArray;
	}

	public abstract EventAlias getEventAlias();
	
	/**
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		epsSet.add(this);
	}
	*/
	
	/**
	@Override
	public void toStringBuilderWithNickName(StringBuilder sw) {
		sw.append(this.eventNickName+"."+this.nickName);
	}
	*/
	
	//public abstract int getRelation(EventOrPropertySpecification eps);
	/*
	public static int getRelation(EventOrPropertySpecification e1, EventOrPropertySpecification e2){
		if((e1 instanceof EventSpecification) && 
			(e2 instanceof EventSpecification)){
			return getEventAndEventRelation((EventSpecification)e1, (EventSpecification)e2);
		}
		else if((e1 instanceof EventPropertySpecification) &&
				(e2 instanceof EventPropertySpecification)){
			return getPropertyAndPropertyRelation((EventPropertySpecification)e1, (EventPropertySpecification)e2);
		}
		else if((e1 instanceof EventSpecification) && 
				(e2 instanceof EventPropertySpecification)){
			return getEventAndPropertyRelation((EventSpecification)e1, (EventPropertySpecification)e2);
		}
		else{
			int r=getEventAndPropertyRelation((EventSpecification)e2, (EventPropertySpecification)e1);
			return Relation.reverse(r);
		}
	}
	
	public static int getEventAndEventRelation(EventSpecification es1, EventSpecification es2){
//		if( (es1 instanceof EventIndexedSpecification) &&
//			(es2 instanceof EventIndexedSpecification)){
		if(es1.getClass().getSimpleName().equals(es2.getClass().getSimpleName())){
			if(es1.equals(es2)){
				return Relation.EQUAL;
			}
			else{
				return Relation.NONE;
			}
		}
		else if((es1 instanceof EventSpecification) &&
				(es2 instanceof EventIndexedSpecification)){
			return getEventAndEventIndexedRelation((EventSpecification)es1, (EventIndexedSpecification)es2);
		}
		else{
			int r=getEventAndEventIndexedRelation((EventSpecification)es2, (EventIndexedSpecification)es1);
			return Relation.reverse(r);
		}
	}
	
	public static int getEventAndEventIndexedRelation(EventSpecification es, EventIndexedSpecification eis){
		if(es.getEventAlias().equals(eis.getEventAlias())){
			return Relation.CONTAINS;
		}
		return Relation.NONE;
	}
	
	public static int getEventAndPropertyRelation(EventSpecification es, EventPropertySpecification eps){
		if(eps.getEventSpecification() instanceof EventIndexedSpecification){
			return getEventAndEventIndexedRelation(es, (EventIndexedSpecification)eps.getEventSpecification());
		}
		else if(es.equals(eps.getEventSpecification())){
			return Relation.CONTAINS;
		}
		return Relation.NONE;
	}
	
	public static int getPropertyAndPropertyRelation(EventPropertySpecification eps1, EventPropertySpecification eps2){
//		if( ((eps1 instanceof EventPropertyIndexedSpecification) &&
//			(eps2 instanceof EventPropertyIndexedSpecification)) ||
//			((eps1 instanceof EventPropertySpecification) &&
//			(eps2 instanceof EventPropertySpecification))){
		if(eps1.getClass().getSimpleName().equals(eps2.getClass().getSimpleName())){
			if(eps1.equals(eps2)){
				return Relation.EQUAL;
			}
			else{
				return Relation.NONE;
			}
		}
		else if((eps1 instanceof EventPropertySpecification) &&
				(eps2 instanceof EventPropertyIndexedSpecification)){
			return getPropertyAndPropertyIndexedRelation((EventPropertySpecification)eps1, 
					(EventPropertyIndexedSpecification)eps2);
		}
		else{
			int r=getPropertyAndPropertyIndexedRelation((EventPropertySpecification)eps2, 
					(EventPropertyIndexedSpecification)eps1);
			return Relation.reverse(r);
		}
		
	}
	
	public static int getPropertyAndPropertyIndexedRelation(EventPropertySpecification eps, EventPropertyIndexedSpecification epis){
		if(eps.getEventSpecification().equals(epis.getEventSpecification()) &&
				eps.getEventProperty().equals(epis.getEventProperty())){
			return Relation.CONTAINS;
		}
		return Relation.NONE;
	}
	*/
}
