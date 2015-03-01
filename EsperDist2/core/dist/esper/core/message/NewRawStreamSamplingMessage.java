package dist.esper.core.message;

import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.id.WorkerId;
import dist.esper.event.Event;

public class NewRawStreamSamplingMessage extends AbstractMessage{
	private static final long serialVersionUID = 2875374357231716019L;
	RawStream rawStream;

	public NewRawStreamSamplingMessage() {
		super();
	}

	public NewRawStreamSamplingMessage(String sourceId, RawStream rawStream) {
		super(sourceId);
		this.rawStream = rawStream;
	}

	public RawStream getRawStream() {
		return rawStream;
	}

	public void setRawStream(RawStream rawStream) {
		this.rawStream = rawStream;
	}
}
