package dist.esper.core.message;

import java.util.ArrayList;
import java.util.List;

public class StartSubscribeMessage extends AbstractMessage {
	private static final long serialVersionUID = 6779577168170533005L;
	String streamName;
	//String streamEventTypeName;
	List<String> selectElementNameList;
	
	public StartSubscribeMessage() {
		super();
	}
	public StartSubscribeMessage(String sourceId, String streamName, 
			String streamEventTypeName, 
			List<String> selectElementNameList) {
		super(sourceId);
		this.primaryType = PrimaryTypes.SUBSCRIBE;
		this.streamName = streamName;
		//this.streamEventTypeName = streamEventTypeName;
		this.selectElementNameList = new ArrayList<String>(selectElementNameList.size());
		this.selectElementNameList.addAll(selectElementNameList);
	}
	public String getStreamName() {
		return streamName;
	}
	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public List<String> getSelectElementNameList() {
		return selectElementNameList;
	}
	public void setSelectElementNameList(List<String> selectElementName) {
		this.selectElementNameList = selectElementName;
	}
}
