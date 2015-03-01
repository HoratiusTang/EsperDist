package dist.esper.core.flow.stream;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import dist.esper.core.id.WorkerId;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.util.DeepCloneFactory;
import dist.esper.event.Event;

/** 
 * the abstract stream class in @StreamTree,
 * it's the super-class of all other streams.
 * 
 * @author tjy
 *
 */
public abstract class Stream implements Serializable {
	private static final long serialVersionUID = -237125059137909024L;
	static AtomicLong UID=new AtomicLong(0L);
	public static DeepCloneFactory cloner=new DeepCloneFactory();
	
	long eplId;
	String uniqueName;
	Event internalCompositeEvent=null;
	WorkerId workerId;
	List<SelectClauseExpressionElement> resultElementList=new ArrayList<SelectClauseExpressionElement>(4);
	protected long id;
	double rate;//us
	
	public Stream() {
		super();
		id=UID.getAndIncrement();
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public double getRate() {
		return rate;
	}

	public void setRate(double rate) {
		this.rate = rate;
	}

	public abstract int getBaseStreamCount();
	
	public abstract int getRawStreamCount();
	
	/*raw=0, filter=1*/
	public abstract int getLevel();
	
	public List<String> getResultElementUniqueNameList(){
		List<String> nameList=new ArrayList<String>(resultElementList.size());
		for(SelectClauseExpressionElement se: resultElementList){
			nameList.add(se.getUniqueName());
		}
		return nameList;
	}
	public void setResultElementList(
			List<SelectClauseExpressionElement> resultElementList) {
		this.resultElementList.addAll(resultElementList);
	}
	public List<SelectClauseExpressionElement> getResultElementList(){
		return resultElementList;
	}
	public void addResultElement(SelectClauseExpressionElement resultElement){
		this.resultElementList.add(resultElement);
	}
	public WorkerId getWorkerId() {
		return workerId;
	}
	public void setWorkerId(WorkerId workerId) {
		this.workerId = workerId;
	}	
	public long getEplId() {
		return eplId;
	}
	public void setEplId(long eplId) {
		this.eplId = eplId;
	}
	public String getUniqueName() {
		return uniqueName;
	}
	public void setUniqueName(String uniqueName) {
		this.uniqueName = uniqueName;
	}
	public Event getInternalCompositeEvent() {
		return internalCompositeEvent;
	}
	public void setInternalCompositeEvent(Event event) {
		this.internalCompositeEvent = event;
	}
	
	public abstract void toStringBuilder(StringBuilder sw, int indent);
	
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw, 0);
		return sw.toString();
	}
	
	public Set<EventOrPropertySpecification> dumpResultEventOrPropertySpecReferences(){
		Set<EventOrPropertySpecification> epsSet=new HashSet<EventOrPropertySpecification>();
		this.dumpResultEventOrPropertySpecReferences(epsSet);
		return epsSet;
	}
	
	public void dumpResultEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet){
		for(SelectClauseExpressionElement result: resultElementList){
			result.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}	
}
