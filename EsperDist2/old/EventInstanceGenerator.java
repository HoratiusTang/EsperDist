package dist.esper.external;

import java.util.Map;
import java.util.TreeMap;

import dist.esper.event.Event;
import dist.esper.event.EventProperty;

@Deprecated
public class EventInstanceGenerator {
	Event event;
	TreeMap<String,Object> map1;
	TreeMap<String,Object> map2;
	int strCounter=0;
	
	public EventInstanceGenerator(Event event) {
		super();
		this.event = event;
		this.map1=new TreeMap<String,Object>();
		this.map2=new TreeMap<String,Object>();
		init();
	}
	
	public void init(){
		for(EventProperty prop: event.getPropList()){
			Object obj=getInitValue((Class<?>)prop.getType());
			map1.put(prop.getName(), obj); 
		}
	}

	public TreeMap<String,Object> nextEventInstance(){
		for(Map.Entry<String, Object> e: map1.entrySet()){
			Object obj=nextObject(e.getValue());
			map2.put(e.getKey(), obj);
		}
		TreeMap<String,Object> temp=map2;
		map2=map1;
		map1=temp;
		return map1;
	}
	
	public Object nextObject(Object obj){
		if(obj instanceof Integer){
			return next((Integer)obj);
		}
		else if(obj instanceof Long){
			return next((Long)obj);
		}
		else if(obj instanceof String){
			return next((String)obj);
		}
		else if(obj instanceof int[]){
			return next((int[])obj);
		}
		else if(obj instanceof long[]){
			return next((long[])obj);
		}
		else if(obj instanceof Integer[]){
			return next((Integer[])obj);
		}
		else if(obj instanceof Long[]){
			return next((Long[])obj);
		}
		return obj;
	}
	
	public Object getInitValue(Class<?> type){
		if(type.getSimpleName().equals("int") ||
				type.getSimpleName().equals("Integer")){
			return Integer.valueOf(1);
		}
		else if(type.getSimpleName().equals("long") ||
				type.getSimpleName().equals("Long")){
			return Long.valueOf(10L);
		}
		else if(type.getSimpleName().equals("double") ||
				type.getSimpleName().equals("Double")){
			return Double.valueOf(20.0d);
		}
		else if(type.getSimpleName().equals("String")){
			return "str_";
		}
		else if(type.getSimpleName().equals("int[]")){
			int[] ns=new int[3];
			for(int i=0;i<ns.length;i++){
				ns[i]=100+i;
			}
			return ns;
		}
		else if(type.getSimpleName().equals("Integer[]")){
			Integer[] ns=new Integer[3];
			for(int i=0;i<ns.length;i++){
				ns[i]=100+i;
			}
			return ns;
		}
		else if(type.getSimpleName().equals("long[]")){
			long[] ns=new long[3];
			for(int i=0;i<ns.length;i++){
				ns[i]=Integer.valueOf(100+i);
			}
			return ns;
		}
		else if(type.getSimpleName().equals("Long[]")){
			Long[] ns=new Long[3];
			for(int i=0;i<ns.length;i++){
				ns[i]=Long.valueOf(100L+i);
			}
			return ns;
		}
		return new Object();
	}
	
	public Integer next(Integer n){
		return Integer.valueOf(n.intValue()+1);
	}
	
	public Long next(Long n){
		return Long.valueOf(n.longValue()+1);
	}
	
	public String next(String str){
		String nextStr=String.format("%s_%04d", str.substring(0, str.indexOf("_")), strCounter);
		strCounter++;
		return nextStr;
	}
	
	public int[] next(int[] ns){
		for(int i=0;i<ns.length;i++){
			ns[i]=ns[i]+1;
		}
		return ns;
	}
	
	public long[] next(long[] ns){
		for(int i=0;i<ns.length;i++){
			ns[i]=ns[i]+1;
		}
		return ns;
	}
	
	public Integer[] next(Integer[] ns){
		for(int i=0;i<ns.length;i++){
			ns[i]=Integer.valueOf(ns[i].intValue()+1);
		}
		return ns;
	}
	
	public Long[] next(Long[] ns){
		for(int i=0;i<ns.length;i++){
			ns[i]=Long.valueOf(ns[i].longValue()+1);
		}
		return ns;
	}
}
