package dist.esper.test;

import java.io.Serializable;
import java.util.*;

import dist.esper.io.KryoClassRegister;
import dist.esper.test.util.JavaBeanChecker;
import dist.esper.util.ReflectionFactory;

public class TestReflections {
	public static void main(String[] args){
		//test1();
		testJavaBeans();
		//test2();
	}
	
	public static void test1(){
		List<Class<?>> clazzList=ReflectionFactory.getPackageClasses("dist.esper.topo.cost");
		for(Class<?> clazz: clazzList){
			System.out.println(clazz.getName());
		}
	}
	
	public static void test2(){
		List<Class<?>> clazzList=KryoClassRegister.getSortedClasses();
		for(Class<?> clazz: clazzList){
			System.out.println(clazz.getName());
		}
	}
	
	public static void testJavaBeans(){
		String[] pkgNames=new String[]{
			"dist.esper.event",
			"dist.esper.epl.expr",
			"dist.esper.core.flow.stream",
			"dist.esper.core.flow.container",
			"dist.esper.core.id",
		};
		for(String pkgName: pkgNames){
			try{
				testJavaBeanInPackage(pkgName);
				System.out.format("finish %s\n", pkgName);
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
	public static void testJavaBeanInPackage(String pkgName) throws SecurityException, NoSuchMethodException{
		List<Class<?>> clazzList=ReflectionFactory.getPackageClasses(pkgName);
		for(Class<?> clazz: clazzList){
			//System.out.println(clazz.getName());
			if(KryoClassRegister.isSerializable(clazz)){
				JavaBeanChecker.check(clazz);
				//System.out.format("%s passed\n", clazz.getSimpleName());
			}
		}
	}
}
