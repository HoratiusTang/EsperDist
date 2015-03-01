package dist.esper.util;

public class Tuple3D<T,E,V> extends Tuple2D<T, E> {
	private static final long serialVersionUID = -6783030529653773677L;
	protected V third;
	public Tuple3D(){
	}
	public Tuple3D(T first, E second, V third) {
		super(first, second);
		this.third = third;
	}
	public V getThird() {
		return third;
	}
	public void setThird(V third) {
		this.third = third;
	}
	@Override
	public boolean equals(Object obj){
		if(obj instanceof Tuple3D<?,?,?>){
			Tuple3D<T,E,V> that=(Tuple3D<T,E,V>)obj;
			return this.first.equals(that.getFirst()) && 
					this.second.equals(that.getSecond()) &&
					this.third.equals(that.third);
		}
		return false;
	}
	
	@Override
	public String toString(){
		return String.format("[%s,%s,%s]", first.toString(), second.toString(), third.toString());
	}
}
