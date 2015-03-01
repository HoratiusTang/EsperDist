package dist.esper.test.event;

public interface Event {
	public String getType();
	public int getId();
	public String getName();
	public double getPrice();
}
