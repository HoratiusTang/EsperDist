package dist.esper.io;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;

import dist.esper.core.CoordinatorMain;
import dist.esper.util.Logger2;
import dist.esper.util.ReflectionFactory;

public class KryoClassRegister {
	static Logger2 log=Logger2.getLogger(KryoClassRegister.class);
	static List<Class<?>> allClazzList=null;
	static List<String> pkgNameList=new ArrayList<String>(20);
	
	static{
		String[] pkgNames=new String[]{
				"dist.esper.event",
				"dist.esper.core.id",
				"dist.esper.epl.expr",
				"dist.esper.epl.expr.util",
				"dist.esper.core.plan",
				"dist.esper.core.plan.container",
				"dist.esper.core.message",
				"dist.esper.core.cost",
				"dist.esper.core.comm.socket",
				"dist.esper.io"
		};
		for(String pkgName: pkgNames){
			pkgNameList.add(pkgName);
		}
	}
	
	public static void addPackageName(String pkgName){
		if(!pkgNameList.contains(pkgName)){
			pkgNameList.add(pkgName);
		}
	}
	
	public static int registerClasses(Kryo kryo){
		if(allClazzList==null){
			allClazzList=getSortedClasses();
		}
		
		kryo.setRegistrationRequired(false);//ATT
		kryo.setReferences(true);//ATT
		for(Class<?> clazz: allClazzList){
			//System.out.println(clazz.getSimpleName());
			Registration reg=kryo.register(clazz);
			//log.info("registed class %s, id=%d", clazz.getSimpleName(), reg.getId());
		}
		log.info("registed %d classes for Kryo", allClazzList.size());
		return allClazzList.size();
	}
	
	public static List<Class<?>> getSortedClasses(){
		List<Class<?>> allClazzList=new ArrayList<Class<?>>(80);
		for(String pkgName: pkgNameList){
			List<Class<?>> clazzList=ReflectionFactory.getPackageClasses(pkgName);
			for(Class<?> clazz: clazzList){
				if(isSerializable(clazz)){
					allClazzList.add(clazz);
				}
			}
		}
		List<Class<?>> allClazzList2=new ArrayList<Class<?>>(allClazzList.size());
		for(Class<?> clazz: allClazzList){
			allClazzList2.add(clazz);
			Class<?> arrayClazz=getArrayClassType(clazz);
			allClazzList2.add(arrayClazz);
		}
		
		Collections.sort(allClazzList2, ReflectionFactory.classCmp);
		return allClazzList2;
	}
	
	private static Class<?> getArrayClassType(Class<?> clazz){
		Object array=Array.newInstance(clazz, 0);
		return array.getClass();
	}
	
	public static boolean isSerializable(Class<?> clazz){
//		Class<?> superClazz=clazz.getSuperclass();
//		if(superClazz!=null){
//			if(isSerializable(superClazz)){
//				return true;
//			}
//		}
//		
//		Class<?>[] ints=clazz.getInterfaces();
//		for(Class<?> in: ints){
//			if(in.equals(Serializable.class)){
//				return true;
//			}
//		}
//		return false;
		boolean b;
		b=Serializable.class.isAssignableFrom(clazz);
		return b;
	}
}
