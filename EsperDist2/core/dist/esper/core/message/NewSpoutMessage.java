package dist.esper.core.message;

import dist.esper.event.Event;

public class NewSpoutMessage extends AbstractMessage{
	private static final long serialVersionUID = 2563409971140677445L;
	Event event;
	public NewSpoutMessage() {
		super();
	}
	
	public NewSpoutMessage(String sourceId, Event event) {
		super(sourceId);
		this.event = event;		
	}

	public Event getEvent() {
		return event;
	}

	public void setEvent(Event event) {
		this.event = event;
	}
}
