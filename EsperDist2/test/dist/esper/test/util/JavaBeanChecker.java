package dist.esper.test.util;

import java.lang.reflect.*;

public class JavaBeanChecker {
	public static void check(Class<?> clazz){
		if(clazz.isEnum()){
			return;
		}
		Field[] fields=clazz.getDeclaredFields();//ATT: NOT getFields()!
		try {
			Constructor<?> con=clazz.getConstructor();
			assert(con!=null);
		}
		catch (Exception e1) {			
			e1.printStackTrace();
		}
		for(Field f: fields){
			int m=f.getModifiers();
			if((m & Modifier.STATIC) == 0){
				String name=f.getName();
				//System.out.format("  %s passed\n", name);
				String name2=""+Character.toUpperCase(name.charAt(0))+name.substring(1);
				String getMethName="get"+name2;
				String setMethName="set"+name2;
				
				try {
					Method getMeth = clazz.getMethod(getMethName);
					Method setMeth=clazz.getMethod(setMethName, f.getType());

					assert(getMeth!=null);
					assert(setMeth!=null);
				} catch (SecurityException e) {					
					e.printStackTrace();
				} catch (NoSuchMethodException e) {					
					e.printStackTrace();
				}				
			}
		}
	}
}
