package dist.esper.epl.expr;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.ValueJsonSerializer.class)
public class Value extends AbstractIdentExpression{	
	private static final long serialVersionUID = -1090294951237462469L;
	public DataTypeEnum type=DataTypeEnum.NONE;
	public long intVal=Integer.MIN_VALUE;
	public double floatVal=Double.NaN;
	public String strVal=null;
	public boolean boolVal=false;
	
	public Value() {
		super();
	}
	
	public static Value valueOf(Object obj){
		return new Value(obj);
	}

	public Value(Object obj){
		if(obj instanceof Long ||
				obj instanceof Integer ||
				obj instanceof Short ||
				obj instanceof Byte){
			Number n=(Number)obj;
			setIntVal(n.intValue());
		}
		else if(obj instanceof Double ||
				obj instanceof Float){
			Number n=(Number)obj;
			setFloatVal(n.doubleValue());
		}
		else if(obj instanceof Boolean){
			setBoolVal(((Boolean)obj).booleanValue());
		}
		else{
			setStrVal((String)obj);
		}
	}
	
	public Value(long n){
		setIntVal(n);
	}
	
	public Value(String str){
		setStrVal(str);
	}
	
	public long getIntVal(){
		return intVal;
	}
	
	public void setIntVal(long n){
		type=DataTypeEnum.INT;
		intVal=n;
	}
	
	public double getFloatVal(){
		return floatVal;
	}
	
	public void setFloatVal(double d){
		type=DataTypeEnum.FLOAT;
		floatVal=d;
	}
	
	public boolean getBoolVal(){
		return boolVal;
	}
	
	public void setBoolVal(boolean b){
		type=DataTypeEnum.BOOLEAN;
		boolVal=b;
	}
	
	public String getStrVal(){
		return strVal;
	}
	
	public void setStrVal(String str){
		type=DataTypeEnum.STRING;
		strVal=str;
	}
	
	public DataTypeEnum getType(){
		return type;
	}
	
	public void setType(DataTypeEnum type) {
		this.type = type;
	}

	public Object getValue(){
		if(type==DataTypeEnum.INT){
			return intVal;
		}
		else if(type==DataTypeEnum.FLOAT){
			return floatVal;
		}
		else if(type==DataTypeEnum.BOOLEAN){
			return boolVal;
		}
		else if(type==DataTypeEnum.STRING){
			return strVal;
		}
		return null;
	}
	
	public String getValueStr(){
		if(type==DataTypeEnum.INT){
			return intVal+"";
		}
		else if(type==DataTypeEnum.FLOAT){
			return floatVal+"";
		}
		else if(type==DataTypeEnum.BOOLEAN){
			return boolVal+"";
		}
		else if(type==DataTypeEnum.STRING){
			return strVal;
		}
		return null;
	}
	
	public static int compareValue(Value a,Value b){
		assert(a.type==b.type && a.type!=DataTypeEnum.NONE):String.format("Values are not comparable: %s, %s.",a.toString(),b.toString());		
		if(a.type==DataTypeEnum.INT){
			return (int)(a.intVal-b.intVal);
		}
		else if(a.type==DataTypeEnum.FLOAT){
			return Double.compare(a.floatVal, b.floatVal);
		}
		else if(a.type==DataTypeEnum.BOOLEAN){
			return a.boolVal==!b.boolVal?1:-1;
		}
		else{//if(a.type==Types.STRING_TYPE){
			return a.strVal.compareToIgnoreCase(b.strVal);
		}			
	}
	
	public static String valueArrayToString(Value[] vals){
		StringBuilder sb=new StringBuilder();
		sb.append("[");
		for(Value val: vals){
			sb.append(val.getValueStr()+", ");
		}
		if(vals.length>0){
			sb.setLength(sb.length()-2);
		}
		sb.append("]");
		return sb.toString();
	}
	
	@Override
	public int hashCode(){
		if(type==DataTypeEnum.INT){
			return (int)intVal;
		}
		else if(type==DataTypeEnum.FLOAT){
			long v = Double.doubleToLongBits(floatVal);
			return (int)(v^(v>>>32));
		}
		else if(type==DataTypeEnum.BOOLEAN){
			return boolVal?1231:1237;
		}
		else if(type==DataTypeEnum.STRING){
			return strVal.hashCode();
		}
		return 0;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof Value){
			if(compareValue(this,(Value)obj)==0){
				return true;
			}
		}
		return false;
	}
	
	public String toString(){
		return String.format("[type=%s,intVal=%d,doubleVal=%f,strVal=%s]", type.toString(),intVal,floatVal,strVal);
	}

	/**
	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(this.getValueStr());
	}
	*/

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) {
		// TODO Auto-generated method stub
		return true;
	}	
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitValue(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitValue(this, obj);
	}
}
