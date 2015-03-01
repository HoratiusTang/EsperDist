package dist.esper.util;

import java.util.Arrays;

public class StringUtil {
	public static char[] spaces=null;
	public static int indent=4;
	static{
		spaces=new char[100];
		Arrays.fill(spaces,' ');
	}
	public static String getSpaces(int len){
		return new String(spaces,0,len);
	}	
}
