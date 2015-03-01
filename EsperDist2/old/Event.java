package dist.esper.event;


import java.lang.reflect.Field;
import java.util.*;

import dist.esper.epl.expr.DataTypeEnum;
import dist.esper.epl.sementic.StatementSementicWrapper;

@Deprecated
public class Event{
	public String fullName="";
	public String name="";
	public ArrayList<EventProperty> propList=new ArrayList<EventProperty>(4);
	
	public static EnumSet<DataTypeEnum> dataTypeEnumSet=EnumSet.allOf(DataTypeEnum.class);
	
	public Event(String fullName) {
		super();
		this.fullName = fullName;
	}
	
	
	public ArrayList<EventProperty> getPropList() {
		return propList;
	}


	public void addProperty(EventProperty property) {
		property.setEvent(this);
		if(!propList.contains(property)){
			propList.add(property);
		}
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}


	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public EventProperty getProperty(String propName){
		for(EventProperty p: propList){
			if(p.name.equals(propName)){
				return p;
			}
		}
		return null;
	}
	
	public static class Factory{
		public static Event make(Class<?> clazz){
			Event event=new Event(clazz.getName());
			int lastDotIndex=clazz.getName().lastIndexOf('.');
			event.setName(clazz.getName().substring(lastDotIndex+1));
			Field[] fields=clazz.getDeclaredFields();
			for(Field field: fields){
				Class<?> type=field.getType();
				DataTypeEnum typeEnum=toDataTypeEnum(type);
				EventProperty prop=new EventProperty(event,field.getName(),typeEnum);
				event.addProperty(prop);
			}
			return event;
		}
		
		public static DataTypeEnum toDataTypeEnum(Class<?> type){
			String name=type.getSimpleName();
			//System.out.println(name);
			for(DataTypeEnum dataTypeEnum: dataTypeEnumSet){
				if(name.equals(dataTypeEnum.toString())){
					return dataTypeEnum;
				}
			}
			return DataTypeEnum.NONE;
		}
		
//		public static DataTypeEnum toDataTypeEnum(String typeStr){
//			if(typeStr.equals("long") ||
//				typeStr.equals("int") ||
//				typeStr.equals("short") ||
//				typeStr.equals("byte") ||
//				typeStr.equals("char")){
//				return DataTypeEnum.INT;
//			}
//			else if(typeStr.equals("double") ||
//					typeStr.equals("float")){
//				return DataTypeEnum.DOUBLE;
//			}
//			else if(typeStr.equals("boolean")){
//				return DataTypeEnum.BOOLEAN;
//			}
//			else if(typeStr.equals("java.lang.String")){
//				return DataTypeEnum.STRING;
//			}
//			System.err.format("Unsupported data type: %s", typeStr);
//			return DataTypeEnum.NONE;
//		}
	}

	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw);
		return sw.toString();
	}

	//@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(this.fullName);
		sw.append('[');
		String delimiter="";
		for(EventProperty prop: this.getPropList()){
			sw.append(delimiter);
			prop.toStringBuilder(sw);
			delimiter=", ";
		}
		sw.append(']');
	}

//	@Override
//	public boolean resolve(StatementSementicWrapper ssw, Object param) {
//		// TODO Auto-generated method stub
//		return true;
//	}

	@Override
	public int hashCode(){
		return fullName.hashCode();
	}

	@Override
	public boolean equals(Object obj){
		if(obj instanceof Event){
			return this.fullName.equals(((Event)obj).getFullName());
		}
		return false;
	}

}
