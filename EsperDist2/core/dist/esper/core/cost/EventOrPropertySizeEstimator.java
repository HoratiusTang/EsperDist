package dist.esper.core.cost;

import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.EventIndexedSpecification;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.EventPropertyIndexedSpecification;
import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.util.EventOrPropertySpecReferenceDumper;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;

public class EventOrPropertySizeEstimator {
	public static int AVG_SELECT_ELELEMENT_NAME_LENGTH=8; 
	public static int SERIALIZATION_ADDITION_SIZE=2;
	RawStats rawStats;
	
	public EventOrPropertySizeEstimator(RawStats rawStats) {
		super();
		this.rawStats = rawStats;
	}

	public int computeSizeOfComponentType(Object type){//Event or basic type
		if(type instanceof Class<?>){
			return sizeOfPrimitiveType(((Class<?>)type).getSimpleName());
		}
		else if(type instanceof Event){
			return rawStats.estimateEventSize((Event)type);
		}
		else if(type instanceof String){
			return sizeOfPrimitiveType((String)type);
		}
		return 4;
	}
	
	public static int sizeOfPrimitiveType(String simpleName){
		if(simpleName.equals("byte") || simpleName.equals("Byte")){
			return Byte.SIZE >> 3;
		}
		if(simpleName.equals("char") || simpleName.equals("Character")){
			return Character.SIZE >> 3;
		}
		if(simpleName.equals("short") || simpleName.equals("Short")){
			return Short.SIZE >> 3;
		}
		if(simpleName.equals("int") || simpleName.equals("Integer")){
			return Integer.SIZE >> 3;
		}
		if(simpleName.equals("long") || simpleName.equals("Long")){
			return Long.SIZE >> 3;
		}			
		if(simpleName.equals("float") || simpleName.equals("Float")){
			return Float.SIZE >> 3;
		}
		if(simpleName.equals("double") || simpleName.equals("Double")){
			return Double.SIZE >> 3;
		}
		if(simpleName.equals("boolean") || simpleName.equals("Boolean")){
			return 1;
		}
		return 4;
	}
	
	public int computeSizeOfEventProperty(EventProperty eventProp){
		return rawStats.estimateEventPropertySize(eventProp);
	}
	
	public int computeSelectElementsByteSize(List<SelectClauseExpressionElement> seList){
		int totalSize=0;
		if(seList==null){
			return 0;
		}
		for(SelectClauseExpressionElement se: seList){
			if(se.getSelectExpr() instanceof EventOrPropertySpecification){
				int seSize=computeSizeOfEventOrPropertySpecification((EventOrPropertySpecification)se.getSelectExpr());
				totalSize += seSize;
			}
			else{
				Set<EventOrPropertySpecification> epsSet=EventOrPropertySpecReferenceDumper.dump(se.getSelectExpr());
				EventOrPropertySpecification eps=epsSet.iterator().next();//only one
				int seSize=computeSizeOfEventOrPropertySpecification(eps);
				totalSize += seSize;
			}
			totalSize += SERIALIZATION_ADDITION_SIZE + AVG_SELECT_ELELEMENT_NAME_LENGTH; 
		}
		return totalSize;
	}
	
	public int computeEventOrPropertySpecificationsByteSize(List<EventOrPropertySpecification> epsList){
		int totalSize=0;
		for(EventOrPropertySpecification eps: epsList){
			int seSize=computeSizeOfEventOrPropertySpecification(eps);
			totalSize += seSize;
		}
		return totalSize;
	}
	
	private int computeSizeOfEventOrPropertySpecification(EventOrPropertySpecification eps){		
		if(eps instanceof EventPropertySpecification){
			if(eps instanceof EventPropertyIndexedSpecification){
				EventPropertyIndexedSpecification epis=(EventPropertyIndexedSpecification)eps;
				Object type=epis.getEventProp().getComponentType();
				return computeSizeOfComponentType(type);				
			}
			else{
				EventPropertySpecification eps1=(EventPropertySpecification)eps;
				return computeSizeOfEventProperty(eps1.getEventProp());				
			}
		}
		else{
			if(eps instanceof EventIndexedSpecification){
				EventIndexedSpecification eis=(EventIndexedSpecification)eps;
				return computeSizeOfComponentType(eis.getEventAlias().getEvent());				
			}
			else{
				EventSpecification eis=(EventSpecification)eps;
				if(!eis.isArray()){
					return computeSizeOfComponentType(eis.getEventAlias().getEvent());					
				}
				else{
					return computeSizeOfComponentType(eis.getEventAlias().getEvent())*3;//FIXME					
				}
			}
		}
	}
}
