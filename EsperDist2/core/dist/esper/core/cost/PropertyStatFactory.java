package dist.esper.core.cost;

import dist.esper.core.cost.AbstractPropertyStat.*;
import dist.esper.event.EventProperty;

public class PropertyStatFactory {
	public static <T> AbstractPropertyStat<T> newPropertyStat(EventProperty prop){
		if(prop.isArray()){
			return (AbstractPropertyStat<T>) new ArrayPropertyStat(prop.getName());
		}
		else if(prop.isString()){
			return (AbstractPropertyStat<T>) new StringPropertyStat(prop.getName());
		}
		else{
			String simpleName=prop.getTypeSimpleName();		
			if(simpleName.equals("byte") || simpleName.equals("Byte")){
				return (AbstractPropertyStat<T>) new IntegerPropertyStat(prop.getName(), Byte.class);
			}
//			if(simpleName.equals("char") || simpleName.equals("Character")){
//				return (AbstractPropertyStat<T>) new NumberPropertyStat<Character>(prop.getName(), Character.class);
//			}
			else if(simpleName.equals("short") || simpleName.equals("Short")){
				return (AbstractPropertyStat<T>) new IntegerPropertyStat<Short>(prop.getName(), Short.class);
			}
			else if(simpleName.equals("int") || simpleName.equals("Integer")){
				return (AbstractPropertyStat<T>) new IntegerPropertyStat<Integer>(prop.getName(), Integer.class);
			}
			else if(simpleName.equals("long") || simpleName.equals("Long")){
				return (AbstractPropertyStat<T>) new IntegerPropertyStat<Long>(prop.getName(), Long.class);
			}			
			else if(simpleName.equals("float") || simpleName.equals("Float")){
				return (AbstractPropertyStat<T>) new FloatPropertyStat<Float>(prop.getName(), Float.class);
			}
			else if(simpleName.equals("double") || simpleName.equals("Double")){
				return (AbstractPropertyStat<T>) new FloatPropertyStat<Double>(prop.getName(), Double.class);
			}
//			else if(simpleName.equals("boolean") || simpleName.equals("Boolean")){
//				return (AbstractPropertyStat<T>) new NumberPropertyStat<Boolean>(prop.getName(), Boolean.class);
//			}
		}
		return null;
	}
}
