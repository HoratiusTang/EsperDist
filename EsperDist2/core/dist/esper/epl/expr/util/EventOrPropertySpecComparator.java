package dist.esper.epl.expr.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventIndexedSpecification;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.EventPropertyIndexedSpecification;
import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.util.ExpressionComparator.CompareStrategy;

public class EventOrPropertySpecComparator {
	public static enum CompareStrategy{
		EVENT_MATCH,
		EVENTALIAS_MATCH,
		REPLACE_EVENTALIAS_MATCH,
	}
	public static enum EPSRelation{
		NONE("none"),
		IS_CONTAINED("is_contained"),
		EQUAL("equal"),
		CONTAINS("contains");
		
		String str;
		EPSRelation(String str){
			this.str=str;
		}
		@Override
		public String toString(){
			return str;
		}
		
		public EPSRelation reverse(){
			switch(this){
			case NONE:
			case EQUAL:
				return this;
			case IS_CONTAINED:
				return CONTAINS;
			case CONTAINS:
				return IS_CONTAINED;
			default:
				return NONE;
			}
		}
	}
	CompareStrategy compareStrategy;
	Map<EventAlias, EventAlias> eaMap=new HashMap<EventAlias, EventAlias>(2);//replace the second EventAlias
	
	public EventOrPropertySpecComparator(){
		this(CompareStrategy.EVENTALIAS_MATCH, null);
	}
	
	public EventOrPropertySpecComparator(CompareStrategy compareStrategy, Map<EventAlias, EventAlias> eventAliasMap) {
		super();
		this.compareStrategy = compareStrategy;
		if(eventAliasMap!=null){
			this.eaMap=eventAliasMap;
		}
	}
	
	public void setCompareStrategy(CompareStrategy compareStrategy) {
		this.compareStrategy = compareStrategy;
	}

	public void setEventAliasMap(Map<EventAlias, EventAlias> eaMap) {
		this.eaMap = eaMap;
	}

	public boolean compare(EventAlias a, EventAlias b){
		if(a.getEvent().equals(b.getEvent())){
			if(compareStrategy==CompareStrategy.EVENT_MATCH){
				return true;
			}
			EventAlias b2=b;
			if(compareStrategy==CompareStrategy.REPLACE_EVENTALIAS_MATCH){
				b2=eaMap.get(b);
				if(b2==null){
					return false;
				}
			}
			if(a.getEventAsName()==null && b2.getEventAsName()==null){
				return true;
			}
			else if((a.getEventAsName()!=null && b2.getEventAsName()!=null) &&
					a.getEventAsName().equals(b2.getEventAsName())){
				return true;
			}
		}
		return false;
	}
	private static Class<?>[] clazzes={
		EventSpecification.class,
		EventIndexedSpecification.class,
		EventPropertySpecification.class,
		EventPropertyIndexedSpecification.class
	};
	
	private static int getIndex(EventOrPropertySpecification e){
		for(int i=0;i<clazzes.length;i++){
			if(e.getClass().getSimpleName().equals(clazzes[i].getSimpleName())){
				return i;
			}
		}
		return -1;
	}
	
	private static Method[][] methods;
	
	static{
		Class<?> thisClazz=EventOrPropertySpecComparator.class;
		methods=new Method[clazzes.length][];
		for(int i=0;i<methods.length;i++){
			methods[i]=new Method[clazzes.length];
			for(int j=i;j<clazzes.length;j++){
				try {
					methods[i][j]=thisClazz.getMethod("compare"+i+j, clazzes[i], clazzes[j]);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public EPSRelation compare(EventOrPropertySpecification e1, EventOrPropertySpecification e2){
		int index1=getIndex(e1);
		int index2=getIndex(e2);
		boolean reversed=false;
		if(index2<index1){
			int temp=index1; index1=index2; index2=temp;
			EventOrPropertySpecification e3=e1; e1=e2; e2=e3;
			reversed=true;
		}
		try {
			EPSRelation result=(EPSRelation) methods[index1][index2].invoke(this, e1, e2);
			return reversed?result.reverse():result;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare00(EventSpecification e1, EventSpecification e2){
		if(compare(e1.getEventAlias(), e2.getEventAlias())){
			return EPSRelation.EQUAL;
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare01(EventSpecification e1, EventIndexedSpecification e2){
		if(compare(e1.getEventAlias(), e2.getEventAlias())){
			return EPSRelation.CONTAINS;
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare02(EventSpecification e1, EventPropertySpecification e2){
		if(compare00(e1, e2.getEventSpec())==EPSRelation.EQUAL){
			return EPSRelation.CONTAINS;
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare03(EventSpecification e1, EventPropertyIndexedSpecification e2){		
		return compare02(e1, e2);
	}
	
	public EPSRelation compare11(EventIndexedSpecification e1, EventIndexedSpecification e2){
		if(e1.getIndex()==e2.getIndex() && compare00(e1, e2)==EPSRelation.EQUAL){
			return EPSRelation.EQUAL;
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare12(EventIndexedSpecification e1, EventPropertySpecification e2){
		if(e2.getEventSpec() instanceof EventIndexedSpecification){
			if(compare11(e1, (EventIndexedSpecification)e2.getEventSpec())==EPSRelation.EQUAL){
				return EPSRelation.CONTAINS;
			}
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare13(EventIndexedSpecification e1, EventPropertyIndexedSpecification e2){
		return compare12(e1, e2);
	}
	
	public EPSRelation compare22(EventPropertySpecification e1, EventPropertySpecification e2){
		if(e1.getEventProp().equals(e2.getEventProp())){
			String esClassName1=e1.getEventSpec().getClass().getSimpleName();
			String esClassName2=e2.getEventSpec().getClass().getSimpleName();
			if(esClassName1.equals(esClassName2)){
				if(esClassName1.equals(EventSpecification.class.getSimpleName()) && 
					compare00(e1.getEventSpec(), e2.getEventSpec())==EPSRelation.EQUAL){
					return EPSRelation.EQUAL;
				}
				else if(esClassName1.equals(EventIndexedSpecification.class.getSimpleName()) && 
					compare11((EventIndexedSpecification)e1.getEventSpec(), (EventIndexedSpecification)e2.getEventSpec())
							==EPSRelation.EQUAL){
					return EPSRelation.EQUAL;
				}
			}
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare23(EventPropertySpecification e1, EventPropertyIndexedSpecification e2){
		if(compare22(e1, e2)==EPSRelation.EQUAL){
			return EPSRelation.CONTAINS;
		}
		return EPSRelation.NONE;
	}
	
	public EPSRelation compare33(EventPropertyIndexedSpecification e1, EventPropertyIndexedSpecification e2){
		if(e1.getIndex()==e2.getIndex() && compare22(e1, e2)==EPSRelation.EQUAL){
			return EPSRelation.EQUAL;
		}
		return EPSRelation.NONE;
	}
}
