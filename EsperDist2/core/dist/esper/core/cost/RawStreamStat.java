package dist.esper.core.cost;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import dist.esper.event.Event;
import dist.esper.event.EventProperty;
import dist.esper.util.StringUtil;

public class RawStreamStat implements Serializable{	
	private static final long serialVersionUID = 2324999403003547473L;
	String eventName;
	long startTimestampUS;//us
	long lastTimestampUS;//us
	long eventCount=0;
	long batchCount=0;
	transient Map<String, AbstractPropertyStat<?>> propStatMap=new HashMap<String, AbstractPropertyStat<?>>();
	
	public RawStreamStat() {
		super();
	}

	public RawStreamStat(String eventName) {
		super();
		this.eventName = eventName;
		this.startTimestampUS = System.nanoTime()/1000;
		this.lastTimestampUS = startTimestampUS+1;
	}
	
	public RawStreamStat(Event event) {
		this(event.getName());
		for(EventProperty prop: event.getPropList()){
			propStatMap.put(prop.getName(), PropertyStatFactory.newPropertyStat(prop));
		}
	}
	
	public void tryUpdateBatch(Object[] tuples){
		Map<String,Object> m=null;
		for(Object t: tuples){
			m=(Map<String,Object>)t;
			eventCount++;
			if(eventCount>AbstractPropertyStat.DEFUALT_SAMPLE_SIZE){
				if(Math.random() > (double)AbstractPropertyStat.DEFUALT_SAMPLE_SIZE/(double)eventCount){
					continue;
				}
			}
			for(Map.Entry<String, Object> e: m.entrySet()){
				AbstractPropertyStat<?> ps=propStatMap.get(e.getKey());
				ps.update(e.getValue());
			}
		}
		batchCount++;
		lastTimestampUS=System.nanoTime()/1000;
	}
	
	public String getEventName(){
		return eventName;
	}
	
	public AbstractPropertyStat<?> getPropertyStat(String propName){
		return propStatMap.get(propName);
	}
	
	public long durationUS(){
		return lastTimestampUS-startTimestampUS;
	}
	
	public double getOutputRateSec(){
		long duration=durationUS();
		if(duration==0){
			return 0.0d;				
		}
		return (double)eventCount*1e6/(double)duration;
	}
	
	public void toStringBuilder(StringBuilder sb, int indent){
		sb.append(StringUtil.getSpaces(indent));
		sb.append("RawStreamStat[");
		sb.append("eventName="); sb.append(eventName);
		sb.append(", eventCount="); sb.append(eventCount);
		sb.append(", batchCount="); sb.append(batchCount);
		sb.append("]");
		for(AbstractPropertyStat<?> ps: propStatMap.values()){
			sb.append('\n');
			sb.append(StringUtil.getSpaces(indent+4));
			ps.toStringBuilder(sb);
		}
	}

	public long getStartTimestampUS() {
		return startTimestampUS;
	}

	public void setStartTimestampUS(long startTimestampUS) {
		this.startTimestampUS = startTimestampUS;
	}

	public long getLastTimestampUS() {
		return lastTimestampUS;
	}

	public void setLastTimestampUS(long lastTimestampUS) {
		this.lastTimestampUS = lastTimestampUS;
	}

	public long getEventCount() {
		return eventCount;
	}

	public void setEventCount(long eventCount) {
		this.eventCount = eventCount;
	}

	public long getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(long batchCount) {
		this.batchCount = batchCount;
	}

	public Map<String, AbstractPropertyStat<?>> getPropStatMap() {
		return propStatMap;
	}

	public void setPropStatMap(Map<String, AbstractPropertyStat<?>> propStatMap) {
		this.propStatMap = propStatMap;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}
	
}
