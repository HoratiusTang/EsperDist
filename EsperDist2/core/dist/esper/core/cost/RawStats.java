package dist.esper.core.cost;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import dist.esper.core.cost.AbstractPropertyStat.*;
import dist.esper.core.cost.SampleNumberFilterSimulator.FilterSimulator;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.epl.expr.Value;
import dist.esper.event.*;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

public class RawStats {
	static Logger2 log=Logger2.getLogger(RawStats.class);
	public static double DEFAULT_OUTPUT_RATE=10.0d;
	Map<String, RawStreamStat> rawStreamStatMap=new ConcurrentHashMap<String, RawStreamStat>();//indexed by event name
	
	public RawStats() {
		super();
	}
	
	public Map<String, RawStreamStat> getRawStreamStatMap() {
		return rawStreamStatMap;
	}
	
	public void setRawStreamStatMap(Map<String, RawStreamStat> rawStreamStatMap) {
		this.rawStreamStatMap = rawStreamStatMap;
	}
	
	public RawStreamStat getRawStreamStatBlocked(String eventName){
		RawStreamStat rss=rawStreamStatMap.get(eventName);
		while(rss==null){
			log.debug("wait to get RawStreamStat for Event "+eventName);
			ThreadUtil.sleep(1000);
			rss=rawStreamStatMap.get(eventName);
		}
		return rss;
	}

	public double getOutputRateSec(RawStream rsl){
		return getOutputRateSec(rsl.getEvent());
	}

	public double getOutputRateSec(Event event){
		//RawStreamStat rsc=rawStreamStatMap.get(event.getName());
		RawStreamStat rsc=getRawStreamStatBlocked(event.getName());
		if(rsc==null){
			return DEFAULT_OUTPUT_RATE;
		}
		return rsc.getOutputRateSec();
	}
	
	public int estimateEventSize(Event event){
		//RawStreamStat rsc=rawStreamStatMap.get(event.getName());
		RawStreamStat rsc=getRawStreamStatBlocked(event.getName());
		int eventSize=0;
		for(EventProperty prop: event.getPropList()){
			int propSize=estimateEventPropertySize(prop, rsc);
			eventSize+=propSize;
		}			
		return eventSize;		
	}
	
	public <T> List<T> getSampleValues(EventProperty prop){
		//RawStreamStat rsc=rawStreamStatMap.get(prop.getEvent().getName());
		RawStreamStat rsc=getRawStreamStatBlocked(prop.getEvent().getName());
		AbstractPropertyStat<?> ps=rsc.getPropertyStat(prop.getName());
		if(ps instanceof ValuePropertyStat<?>){
			return (List<T>)((ValuePropertyStat<?>)ps).getSampleValues();
		}
		return null;
	}
	
	public double estimateAbsoluteSelectFactor(EventProperty prop, Value value, OperatorTypeEnum op){
		//RawStreamStat rsc=rawStreamStatMap.get(prop.getEvent().getName());
		RawStreamStat rsc=getRawStreamStatBlocked(prop.getEvent().getName());
		if(rsc==null){
			return Double.NEGATIVE_INFINITY;
		}
		AbstractPropertyStat<?> ps=rsc.getPropertyStat(prop.getName());
		if(!(ps instanceof ValuePropertyStat)){
			return 0.0d;
		}
		ValuePropertyStat<?> vps=(ValuePropertyStat<?>)ps;
		FilterSimulator fs=SampleNumberFilterSimulator.getFilterSimlulator(prop);
		fs.setFirst(vps.getSampleValues());
		fs.setSecond((Number)vps.getMin());
		fs.setThird((Number)vps.getMax());
		fs.setFourth(value);
		fs.setOperator(op);
		
		double sf=fs.estimateSelectFactor();		
		return sf>=0.0d?sf:AbstractPropertyStat.DEFAULT_PROBABILITY;
	}
	
	public int estimateEventPropertySize(EventProperty prop){
		//RawStreamStat rsc=rawStreamStatMap.get(prop.getEvent().getName());
		RawStreamStat rsc=getRawStreamStatBlocked(prop.getEvent().getName());
		return estimateEventPropertySize(prop, rsc);
	}
	
	public int estimateEventPropertySize(EventProperty prop, RawStreamStat rsc){
		if(rsc==null){
			//rsc=rawStreamStatMap.get(prop.getEvent().getName());
			rsc=getRawStreamStatBlocked(prop.getEvent().getName());
		}
		AbstractPropertyStat<?> ps=rsc.propStatMap.get(prop.getName());
		if(prop.isArray()){
			Object cpnType=prop.getComponentType();
			int cpnSize=sizeOfComponentType(cpnType);
			ArrayPropertyStat aps=(ArrayPropertyStat)ps;
			return (int)(aps.getAvgLength() * (double)cpnSize)+1;
		}
		else if(prop.isString()){
			StringPropertyStat sps=(StringPropertyStat)ps;
			return (int)(sps.getAvgLength() * (double)(Character.SIZE >> 3))+1;
		}
		else{
			return sizeOfComponentType(prop.getType());
		}
	}
	
	public int sizeOfComponentType(Object type){
		if(type instanceof Class<?>){
			return EventOrPropertySizeEstimator.sizeOfPrimitiveType(((Class<?>)type).getSimpleName());
		}
		else if(type instanceof Event){
			return this.estimateEventSize((Event)type);
		}
		else if(type instanceof String){
			return EventOrPropertySizeEstimator.sizeOfPrimitiveType((String)type);
		}
		return 4;
	}
	
	public void updateRawStreamStat(RawStreamStat rss){
		rawStreamStatMap.put(rss.getEventName(), rss);
	}
	
//	public void addRawStreamStat(RawStreamLocation rsl){
//		if(!rscMap.containsKey(rsl.getCompositeEvent().getName())){
//			rscMap.put(rsl.getCompositeEvent().getName(), 
//					new RawStreamStat(rsl.getCompositeEvent()));
//		}
//	}
	
//	public void updateRawStreamStat(
//		InstanceStat insStat, String filterContainerId){//FIXME
//		RawStreamStat rsc=rscMap.get(insStat.uniqueName);
//		if(rsc.duration < insStat.duration()){
//			rsc.duration = insStat.duration();
//		}
//		if(rsc.totalCount < insStat.eventCount){
//			rsc.totalCount = insStat.eventCount;
////			for(PropertyStat ps: insStat.fieldStats){
////				rsc.psMap.put(ps.propName, ps);
////			}
//		}
//	}
}
