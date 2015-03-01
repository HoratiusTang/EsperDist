package dist.esper.core.flow.stream;


import java.util.ArrayList;
import java.util.List;

import dist.esper.core.id.WorkerId;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.EventSpecification;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;

/**
 * the source stream comes from external data source.
 * distinguished from @DerivedStream.
 * 
 * @author tjy
 *
 */
public class RawStream extends Stream{
	private static final long serialVersionUID = 5370034333019000678L;
	Event event;
	
	public RawStream() {
		super();
	}

	public RawStream(RawStream other){
		this.event = other.event;
		this.workerId = other.workerId;
		this.uniqueName = event.getName();
		initResultElementList();
	}
	
	public RawStream(WorkerId workerId, Event event) {
		super();
		this.event = event;
		this.workerId = workerId;
		this.uniqueName = event.getName();
		initResultElementList();
	}
	
	private void initResultElementList(){
		resultElementList.clear();
		EventSpecification es=new EventSpecification(new EventAlias(event));
		for(EventProperty prop: event.getPropList()){			
			EventPropertySpecification eps=new EventPropertySpecification(es, prop);
			SelectClauseExpressionElement se=new SelectClauseExpressionElement(eps);
			resultElementList.add(se);
		}
	}

	public Event getInternalCompositeEvent() {
		return event;
	}

	public void setInternalCompositeEvent(Event event) {
		this.event = event;
	}
	
	public Event getEvent() {
		return event;
	}

	public void setEvent(Event event) {
		this.event = event;
	}

	public String getEventName(){
		return event.getName();
	}

	public List<String> getResultElementUniqueNameList(){
		List<String> nameList=new ArrayList<String>(1);
		nameList.add(event.getName());
		return nameList;
	}
	
//	@Override
//	public List<SelectClauseExpressionElement> getMergedResultElementList(){
//		return this.resultElementList;
//	}
	
	@Override
	public void toStringBuilder(StringBuilder sw, int indent){
		StreamStringlizer.toStringBuilder(this, sw, indent);
	}
	
	@Override
	public int getBaseStreamCount() {
		return 1;
	}

	@Override
	public int getRawStreamCount() {
		return 1;
	}

	@Override
	public int getLevel() {		
		return 0;
	}
}
