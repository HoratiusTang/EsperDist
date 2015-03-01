package dist.esper.util;

import java.io.Serializable;

public class Tuple2D<T,E> implements Serializable{
	private static final long serialVersionUID = 3045421645561593609L;
	protected T first;
	protected E second;
	
	public Tuple2D(){		
	}
	
	public Tuple2D(T first, E second) {
		super();
		this.first = first;
		this.second = second;
	}

	public T getFirst() {
		return first;
	}

	public void setFirst(T first) {
		this.first = first;
	}

	public E getSecond() {
		return second;
	}

	public void setSecond(E second) {
		this.second = second;
	}
	
	@Override
	public int hashCode(){
		return first.hashCode() ^ second.hashCode();
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof Tuple2D<?,?>){
			Tuple2D<T,E> that=(Tuple2D<T,E>)obj;
			return this.first.equals(that.getFirst()) && this.second.equals(that.getSecond());
		}
		return false;
	}
	
	@Override
	public String toString(){
		return String.format("[%s,%s]", first.toString(), second.toString());
	}
}
