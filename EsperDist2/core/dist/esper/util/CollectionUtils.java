package dist.esper.util;

import java.util.*;


public class CollectionUtils {
	public static <K,V> Map<V,K> reverse(Map<K,V> kvMap){
		Map<V,K> vkMap=new HashMap<V,K>(kvMap.size());
		for(Map.Entry<K,V> e: kvMap.entrySet()){
			vkMap.put(e.getValue(), e.getKey());
		}
		return vkMap;
	}
	
	public static <K,V> Map<K,V> clone(Map<K,V> map1){
		Map<K,V> map2=new HashMap<K,V>(map1.size());
		for(Map.Entry<K,V> e: map1.entrySet()){
			map2.put(e.getKey(), e.getValue());
		}
		return map2;
	}
	public static <T1,T2,T3> Map<T1,T3> merge(Map<T1,T2> map1, Map<T2,T3> map2){
		Map<T1,T3> map3=new HashMap<T1,T3>(map1.size());
		merge(map1, map2, map3);
		return map3;
	}
	
	public static <T1,T2,T3> Map<T1,T3> merge(Map<T1,T2> map1, Map<T2,T3> map2, Map<T1,T3> map3){
		for(Map.Entry<T1, T2> e1: map1.entrySet()){
			map3.put(e1.getKey(), map2.get(e1.getValue()));
		}
		return map3;
	}
	
	public static <T> Map<T,T> makeMap(Collection<T> set){
		Map<T,T> map=new HashMap<T,T>(set.size());
		for(T t: set){
			map.put(t,t);
		}
		return map;
	}
	
	public static <T> List<T> shallowClone(Collection<T> set){
		List<T> list=new ArrayList<T>(set.size());
		for(T t: set){
			list.add(t);
		}
		return list;
	}
}
