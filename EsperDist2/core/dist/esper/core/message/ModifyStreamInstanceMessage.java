package dist.esper.core.message;

import dist.esper.core.flow.container.StreamContainer;

public class ModifyStreamInstanceMessage extends AbstractMessage{
	private static final long serialVersionUID = 5138309755460553251L;
	StreamContainer streamContainer;
	
	public ModifyStreamInstanceMessage() {
		super();
	}

	public ModifyStreamInstanceMessage(String sourceId) {
		super(sourceId);
		this.primaryType=PrimaryTypes.CONTROLL;
	}
	
	public ModifyStreamInstanceMessage(String sourceId, StreamContainer streamContainer) {
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
