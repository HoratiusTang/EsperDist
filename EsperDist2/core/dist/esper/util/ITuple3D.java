package dist.esper.util;

public interface ITuple3D<T,E,V> {
	public T getFirst();
	public E getSecond();
	public V getThird();
	
	public void setFirst(T t);
	public void setSecond(E e);
	public void setThird(V v);
}
