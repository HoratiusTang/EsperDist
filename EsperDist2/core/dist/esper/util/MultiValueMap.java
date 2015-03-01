package dist.esper.util;

import java.util.*;

public class MultiValueMap<K,V> extends TreeMap<K,Set<V>>{
	private static final long serialVersionUID = -4480649499352251122L;

	public MultiValueMap() {
		super();		
	}

	public MultiValueMap(Comparator<? super K> comparator) {
		super(comparator);		
	}

	public void putPair(K key, V value){
		Set<V> set=this.get(key);
		if(set==null){
			set=new HashSet<V>();
			this.put(key, set);
		}
		set.add(value);
	}
	
	public boolean containsPair(K key, V value){
		Set<V> set=this.get(key);
		if(set!=null){
			return set.contains(value);
		}
		return false;
	}
	
	public boolean removePair(K key, V value){
		Set<V> set=this.get(key);
		if(set!=null){
			return set.remove(value);
		}
		return false;
	}
}
