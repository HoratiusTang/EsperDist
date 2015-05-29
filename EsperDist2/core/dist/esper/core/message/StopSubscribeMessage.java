//package dist.esper.core.message;
//
//import java.util.List;
//
//public class StopSubscribeMessage extends AbstractMessage {
//	private static final long serialVersionUID = 4262456315877959677L;
//	String streamName;
//	
//	public StopSubscribeMessage() {
//		super();
//	}
//	public StopSubscribeMessage(String sourceId, String streamName, 
//			String streamEventTypeName, 
//			List<String> selectElementNameList) {
//		super(sourceId);
//		this.primaryType = PrimaryTypes.SUBSCRIBE;
//		this.streamName = streamName;		
//	}
//	public String getStreamName() {
//		return streamName;
//	}
//	public void setStreamName(String streamName) {
//		this.streamName = streamName;
//	}
//}
