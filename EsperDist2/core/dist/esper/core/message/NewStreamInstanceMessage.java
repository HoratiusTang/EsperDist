package dist.esper.core.message;

import dist.esper.core.flow.container.StreamContainer;

public class NewStreamInstanceMessage extends AbstractMessage{
	private static final long serialVersionUID = -7798239742265480819L;
	StreamContainer streamContainer;
	
	public NewStreamInstanceMessage() {
		super();
	}

	public NewStreamInstanceMessage(String sourceId) {
		super(sourceId);
		this.primaryType=PrimaryTypes.CONTROLL;
	}
	
	public NewStreamInstanceMessage(String sourceId, StreamContainer streamContainer) {
		this(sourceId);
		this.sourceId = sourceId;
		this.streamContainer = streamContainer;
	}

	public StreamContainer getStreamContainer() {
		return streamContainer;
	}

	public void setStreamContainer(StreamContainer streamContainer) {
		this.streamContainer = streamContainer;
	}
	
}
