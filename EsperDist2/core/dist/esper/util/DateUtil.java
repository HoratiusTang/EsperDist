package dist.esper.util;

import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtil {
	static String DEFAULT_FOMRAT="MMddHHmmss";
	static Map<String, SimpleDateFormat> sdfMap=new TreeMap<String, SimpleDateFormat>();
	public static String formatDate(String format, Date date){
		if(sdfMap.get(format)==null){
			sdfMap.put(format, new SimpleDateFormat(format));
		}
		return sdfMap.get(format).format(date);
	}
	public static String formatDate(String format){
		return formatDate(format, new Date());
	}
	public static String formatDate(){
		return formatDate(DEFAULT_FOMRAT, new Date());
	}
}
