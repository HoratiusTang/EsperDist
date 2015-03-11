package dist.esper.experiment;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * template example:
 * SELECT e%01.n0, e%01.d0, e%01.s0, e%02.n1, e%02.s1 
 * FROM $E#01(m0>$E.m0#01) as e%01, $E#02(m1>$E.m1#02) as e%02
 * WHERE e%01.n1=e%02.n1 and e%01.d0>e%02.d0
 * 
 * @author tjy
 */
public class QueryTemplate {
	public static Pattern eventPattern=Pattern.compile("\\$(\\w+)#\\w+");
	public static Pattern propPattern=Pattern.compile("\\$(\\w+)\\.(\\w+)#\\w+");
	
	static AtomicLong UID=new AtomicLong(0);
	long id;
	String template;
	List<EventVariable> evarList=new ArrayList<EventVariable>();
	List<PropertyVariable> pvarList=new ArrayList<PropertyVariable>();

	public QueryTemplate(String template) {
		super();		
		this.template = template.trim();
		this.id = UID.getAndIncrement();
		this.parse();
	}
	
	public void parse(){
		Matcher em=eventPattern.matcher(template);
		while(em.find()){
			String var=em.group(0);//whole pattern
			//var=var.replace("\\$", "\\\\$");
			var="\\"+var;
			String eventType=em.group(1);
			EventVariable evar=new EventVariable(var, eventType);
			evarList.add(evar);
		}
		
		Matcher pm=propPattern.matcher(template);
		while(pm.find()){
			String var=pm.group(0);//whole pattern
			var="\\"+var;
			String eventType=pm.group(1);
			String propName=pm.group(2);
			PropertyVariable pvar=new PropertyVariable(var, eventType, propName);
			pvarList.add(pvar);
		}
	}
	
	public String generateQuery(Map<String,String> replaceMap, long seq){
		String q=this.template;
		for(Map.Entry<String, String> e: replaceMap.entrySet()){
			q=q.replaceAll(e.getKey(), e.getValue());
		}
		String idStr=String.format("%02d", id);
		String seqStr=String.format("%04d", seq);
		q=q.replaceAll("%", idStr+seqStr);
		return q;
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getTemplate() {
		return template;
	}

	public void setTemplate(String template) {
		this.template = template;
	}

	public List<EventVariable> getEventVariableList() {
		return evarList;
	}

	public List<PropertyVariable> getPropertyVariableList() {
		return pvarList;
	}
	
	public int getEventCount(){
		return evarList.size();
	}
	
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append(id); sb.append(":"); 
		sb.append(template); sb.append(", ");
		sb.append(evarList.toString()); sb.append(", ");
		sb.append(pvarList.toString());
		return sb.toString();
	}

	abstract class Variable{
		String var;
		public Variable(String var) {
			super();
			this.var = var;
		}
		public String getVar() {
			return var;
		}
		public void setVar(String var) {
			this.var = var;
		}
	}
	
	class EventVariable extends Variable{
		String eventCategory;		
		public EventVariable(String var, String eventCategory) {
			super(var);
			this.eventCategory = eventCategory;
		}
		public String getEventCategory() {
			return eventCategory;
		}
		public void setEventCategory(String eventCategory) {
			this.eventCategory = eventCategory;
		}
		@Override
		public String toString(){
			return var+"("+eventCategory+")";
		}
	}
	
	class PropertyVariable extends Variable{
		String eventCategory;
		String propName;		
		public PropertyVariable(String var, String eventCategory, String propName) {
			super(var);
			this.eventCategory = eventCategory;
			this.propName = propName;
		}
		public String getEventCategory() {
			return eventCategory;
		}
		public void setEventCategory(String eventCategory) {
			this.eventCategory = eventCategory;
		}
		public String getPropName() {
			return propName;
		}
		public void setPropName(String propName) {
			this.propName = propName;
		}
		@Override
		public String toString(){
			return var+"("+eventCategory+"."+propName+")";
		}
	}
}
