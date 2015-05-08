package dist.esper.core.message;

import java.io.Serializable;

public class AbstractMessage implements Serializable{
	private static final long serialVersionUID = -8888462073273616064L;
	int primaryType=PrimaryTypes.CONTROLL;
	String sourceId;
	
	public AbstractMessage() {
		super();
	}

	public AbstractMessage(String sourceId) {
		super();
		this.sourceId = sourceId;
	}

	public int getPrimaryType() {
		return primaryType;
	}

	public void setPrimaryType(int primaryType) {
		this.primaryType = primaryType;
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}
	
}
