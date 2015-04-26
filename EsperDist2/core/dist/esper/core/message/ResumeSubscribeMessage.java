package dist.esper.core.message;

import java.util.ArrayList;
import java.util.List;

public class ResumeSubscribeMessage extends AbstractMessage {
	private static final long serialVersionUID = -8126296549165974615L;
	String streamName;
	//String streamEventTypeName;
	List<String> selectElementNameList;
	
	public ResumeSubscribeMessage() {
		super();
	}
	public ResumeSubscribeMessage(String sourceId, String streamName, 
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
