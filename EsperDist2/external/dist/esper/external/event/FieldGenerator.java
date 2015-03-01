package dist.esper.external.event;

public abstract class FieldGenerator {
	protected long nextLong(){
		return 0L;
	}
	protected double nextDouble(){
		return 0.0d;
	}
	public abstract Object next();
}
