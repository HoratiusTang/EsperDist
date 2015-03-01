package dist.esper.core.cost;

public class RawStreamCounter{
	String name;
	long count;
	
	public RawStreamCounter(String name){
		this(name,0L);
	}
	public RawStreamCounter(String name, Long count) {
		this.name=name;
		this.count=count;
	}
	public void increase(long inc){
		count+=inc;
	}
	public String getEventName() {
		return name;
	}
	public void setEventName(String eventName) {
		this.name = eventName;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}
}
