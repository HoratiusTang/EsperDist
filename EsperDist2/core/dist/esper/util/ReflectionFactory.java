package dist.esper.util;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ReflectionFactory {
	public static class ClassComparator implements Comparator<Class<?>>{
		@Override
		public int compare(Class<?> a, Class<?> b) {
			if(!a.isArray() && b.isArray()){
				return -1;
			}
			else if(a.isArray() && !b.isArray()){
				return 1;
			}
			return a.getName().compareTo(b.getName());
		}
	}
	public static ClassComparator classCmp=new ClassComparator();
	public static List<Class<?>> getPackageSortedClasses(String packageName){
		List<Class<?>> classList=getPackageClasses(packageName);
		Collections.sort(classList, classCmp);
		return classList;
	}
	
	public static List<Class<?>> getPackageClasses(String packageName){
		List<Class<?>> classList=Collections.emptyList();
		String packageDirName=packageName.replace('.', '/');
		try{
			Enumeration<URL> packageDirs=Thread.currentThread().getContextClassLoader().getResources(packageDirName);
			
			URL url=null;
			while (packageDirs.hasMoreElements()){
				 url = packageDirs.nextElement();
				 break;
			}
			
			if(url==null){
				return classList;
			}
			
			String protocol=url.getProtocol();
			if(protocol.equalsIgnoreCase("file")){
				File packageDir=new File(url.getFile());
				return getPackageClassesFromDirectory(packageName,packageDir);
			}
			else{//jar file
				JarFile jar=((JarURLConnection)url.openConnection()).getJarFile();
				return getPackageClassesFromJarFile(packageName,jar);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		return classList;
	}
	
	public static List<Class<?>> getPackageClassesFromDirectory(String packageName, File packageDir){
		List<Class<?>> classList=new ArrayList<Class<?>>();
		File[] files=packageDir.listFiles();
		try{
			for(File f: files){
				if(f.getName().endsWith(".class")){
					String className=packageName+"."+f.getName().substring(0,f.getName().length()-6);
					Class<?> clazz=Class.forName(className);
					//System.out.println(clazz.getName());
					classList.add(clazz);
				}
			}
		}
		catch (ClassNotFoundException e) {			
			e.printStackTrace();
		}
		return classList;
	}
	
	public static List<Class<?>> getPackageClassesFromJarFile(String packageName, JarFile jar){
		String packageDirName=packageName.replace('.', '/');
		List<Class<?>> classList=new ArrayList<Class<?>>();
		try{
			Enumeration<JarEntry> entries = jar.entries();  
			while(entries.hasMoreElements()){
				JarEntry entry = entries.nextElement();
				if(entry.getName().startsWith(packageDirName) &&
						entry.getName().endsWith(".class")){					
					int index=entry.getName().lastIndexOf('/');
					String className=packageName+"."+entry.getName().substring(index+1,entry.getName().length()-6);
					Class<?> clazz=Class.forName(className);
					classList.add(clazz);
				}
			}
		}
		catch (ClassNotFoundException e) {			
			e.printStackTrace();
		}
		return classList;
	}
	
}
