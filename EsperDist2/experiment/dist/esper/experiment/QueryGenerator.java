package dist.esper.experiment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import dist.esper.experiment.QueryTemplate.EventVariable;
import dist.esper.experiment.QueryTemplate.PropertyVariable;
import dist.esper.experiment.util.MultiLineFileWriter;
import dist.esper.external.event.FieldGenerator;
import dist.esper.util.MultiValueMap;
import dist.esper.util.Tuple2D;

public class QueryGenerator {
	public List<String> templateStrs;
	public List<List<QueryTemplate>> tempLists;
	public Map<String, Map<String,FieldGenerator>> eventPrototypeMap=new HashMap<String, Map<String,FieldGenerator>>();
	public MultiValueMap<String, String> eventNameMap=new MultiValueMap<String, String>();
	
	public QueryGenerator() {
		super();
		tempLists=new ArrayList<List<QueryTemplate>>();
		for(int i=0;i<=5;i++){
			tempLists.add(new ArrayList<QueryTemplate>());
		}
	}
	
	public void addEventPrototype(String eventCategory, Map<String,FieldGenerator> fgMap){
		this.eventPrototypeMap.put(eventCategory, fgMap);
	}
	
	public void addEventName(String eventCategory, String eventName){
		this.eventNameMap.putPair(eventCategory, eventName);
	}

	public void readTemplatesFromFile(String filePath) throws Exception{
		templateStrs=MultiLineFileWriter.readFromFile(filePath);
		for(String tempStr: templateStrs){
			QueryTemplate qt=new QueryTemplate(tempStr);
			tempLists.get(qt.getEventCount()).add(qt);
		}
	}	
	
	public List<String> generateQuries(List<IntPair> pairList){
		List<String> queryList=new LinkedList<String>();
		int seq=0;
		for(IntPair pair: pairList){
			List<QueryTemplate> tempList=tempLists.get(pair.getFirst());
			if(tempList.size()<=0){
				throw new RuntimeException("the size of template list is 0");
			}
			for(int j=0; j<pair.getSecond(); j++){
				int index= (int)(Math.random() * tempList.size());
				QueryTemplate qt=tempList.get(index);
				String query=generateQuery(qt, seq);
				queryList.add(query);
			}
		}
		return queryList;
	}
	
	public String generateQuery(QueryTemplate qt, int seq){
		List<EventVariable> evarList=qt.getEventVariableList();
		List<PropertyVariable> pvarList=qt.getPropertyVariableList();
		Map<String,String> replaceMap=new TreeMap<String, String>();
		for(EventVariable evar: evarList){
			String eventName=randomChooseEventName(evar.getEventCategory());
			replaceMap.put(evar.getVar(), eventName);
		}
		for(PropertyVariable pvar: pvarList){
			Object num=randomChooseNumber(pvar.getEventCategory(), pvar.getPropName());
			replaceMap.put(pvar.getVar(), num.toString());
		}
		String query=qt.generateQuery(replaceMap, seq);
		return query;
	}
	
	private String randomChooseEventName(String eventCategory){
		Collection<String> eventNames=eventNameMap.get(eventCategory);
		int n=eventNames.size();
		for(String eventName: eventNames){
			if(Math.random() <= 1.0/n){
				return eventName;
			}
			n--;
		}
		return null;//impossible to reach here
	}
	
	
	private Object randomChooseNumber(String eventCategory, String propName){
		Map<String,FieldGenerator> fgMap=this.eventPrototypeMap.get(eventCategory);
		if(fgMap==null){
			throw new RuntimeException(String.format("the event category %s does not exist", eventCategory));
		}
		FieldGenerator fg=fgMap.get(propName);
		if(fg==null){
			throw new RuntimeException(String.format("the property %s does not exist in event category %s", propName, eventCategory));
		}
		return fg.random();
	}
	/**
	 * First is the join-event count, 1 at least.
	 * Second is the query count.
	 * @author tjy
	 */
	public static class IntPair extends Tuple2D<Integer, Integer>{
		private static final long serialVersionUID = -5916807274350558573L;

		public IntPair(Integer first, Integer second) {
			super(first, second);
		}
	}
}

