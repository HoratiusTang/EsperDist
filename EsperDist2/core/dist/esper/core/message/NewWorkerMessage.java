package dist.esper.core.message;

public class NewWorkerMessage extends AbstractMessage {
	private static final long serialVersionUID = -7609660973968538332L;

	public NewWorkerMessage() {
		super();
	}

	public NewWorkerMessage(String sourceId) {
		super(sourceId);
	}
	
}
