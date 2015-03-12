package dist.esper.core.util;

import java.text.NumberFormat;

public class NumberFormatter {
public static NumberFormat numberFormat;
	
	static{
		numberFormat=NumberFormat.getInstance();
		numberFormat.setMaximumFractionDigits(2);
		numberFormat.setGroupingUsed(false);
	}
	
	public static String format(double d){
		return numberFormat.format(d);
	}
	
	public static String format(long n){
		return numberFormat.format(n);
	}
	
	public static String format(Object obj){
		if(obj instanceof Number){
			Number n=(Number)obj;
			if(obj instanceof Byte ||
					obj instanceof Short || 
					obj instanceof Integer || 
					obj instanceof Long){
				return format((long)n.longValue());
			}
			else if(obj instanceof Float ||
					obj instanceof Double){
				return format((double)n.doubleValue());
			}
		}
		return obj.toString();
	}
}
