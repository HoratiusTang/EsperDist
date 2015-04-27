package dist.esper.core.message;

import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@DefaultSerializer(value = ResumeSubscribeMessage.ResumeSubscribeMessageSerializer.class)
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
	
	public static class ResumeSubscribeMessageSerializer extends Serializer<ResumeSubscribeMessage>{
		@Override
		public void write(Kryo kryo, Output output, ResumeSubscribeMessage ssm) {
			kryo.writeObject(output, ssm.getPrimaryType());
			kryo.writeObject(output, ssm.getSourceId());
			kryo.writeObject(output, ssm.getStreamName());
			
			kryo.writeObject(output, ssm.getSelectElementNameList().size());
			for(String sn: ssm.getSelectElementNameList()){
				kryo.writeObject(output, sn);
			}
		}

		@Override
		public ResumeSubscribeMessage read(Kryo kryo, Input input, Class<ResumeSubscribeMessage> type) {
			ResumeSubscribeMessage rsm = new ResumeSubscribeMessage();
			rsm.primaryType = kryo.readObject(input, Integer.class);
			rsm.sourceId = kryo.readObject(input, String.class);
			rsm.streamName = kryo.readObject(input, String.class);
			
			int snCount = kryo.readObject(input, Integer.class);
			rsm.selectElementNameList = new ArrayList<String>(snCount);
			for(int i=0; i<snCount; i++){
				String sn = kryo.readObject(input, String.class);
				rsm.selectElementNameList.add(sn);
			}
			return rsm;
		}
	}
}
