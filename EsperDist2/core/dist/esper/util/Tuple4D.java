package dist.esper.util;

public class Tuple4D<T, E, V, U> extends Tuple3D<T, E, V> {
	private static final long serialVersionUID = -7522968897213654835L;
	protected U fourth;
	public Tuple4D(){		
	}
	public Tuple4D(T first, E second, V third, U fourth) {
		super(first, second, third);
		this.fourth=fourth;
	}
	public U getFourth() {
		return fourth;
	}
	public void setFourth(U fourth) {
		this.fourth = fourth;
	}
	@Override
	public boolean equals(Object obj){
		if(obj instanceof Tuple4D<?,?,?,?>){
			Tuple4D<T,E,V,U> that=(Tuple4D<T,E,V,U>)obj;
			return this.first.equals(that.getFirst()) && 
					this.second.equals(that.getSecond()) &&
					this.third.equals(that.getThird()) &&
					this.fourth.equals(that.fourth);
		}
		return false;
	}
	
	@Override
	public String toString(){
		return String.format("[%s,%s,%s,%s]", first.toString(), second.toString(), third.toString(), fourth.toString());
	}
}
