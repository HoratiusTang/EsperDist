package dist.esper.core.message;

public class DataMessage extends AbstractMessage{
	private static final long serialVersionUID = -9095742802503610806L;
	String streamName;
	String[] elementNames;
	Object[] data;
	
	public DataMessage() {
		super();
	}

	public DataMessage(String sourceId, String streamName,
			String[] eleNames,
			Object[] data) {
		super(sourceId);
		this.primaryType = PrimaryTypes.DATA;
		this.streamName = streamName;
		this.elementNames = new String[eleNames.length];
		System.arraycopy(eleNames,0,
				this.elementNames,0,eleNames.length);
		this.data = data;
	}
	
	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}
	
	public String[] getElementNames() {
		return elementNames;
	}

	public void setElementNames(String[] elementNames) {
		this.elementNames = elementNames;
	}

	public void setData(Object[] data) {
		this.data = data;
	}

	public Object[] getData() {
		return data;
	}
	public void setData(Object[][] data) {
		this.data = data;
	}
}
