package dist.esper.core.message;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@DefaultSerializer(value = NewOrModifyInstanceResponseMessage.NewOrModifyInstanceResponseMessageSerializer.class)
public class NewOrModifyInstanceResponseMessage extends AbstractMessage {
	private static final long serialVersionUID = -1834201342952709481L;
	String streamName;
	long eqlId;
	boolean success;
	
	public NewOrModifyInstanceResponseMessage() {
		super();
		this.primaryType = PrimaryTypes.RESPONSE;
	}	
	
	public NewOrModifyInstanceResponseMessage(String sourceId, String streamName, long eqlId, boolean success) {
		super();
		this.primaryType = PrimaryTypes.RESPONSE;
		this.sourceId = sourceId;
		this.streamName = streamName;
		this.eqlId = eqlId;
		this.success = true;
	}

	public String getStreamName() {
		return streamName;
	}

	public void setStreamName(String streamName) {
		this.streamName = streamName;
	}

	public long getEqlId() {
		return eqlId;
	}
	
	public void setEqlId(long eqlId) {
		this.eqlId = eqlId;
	}	

	public boolean isSuceess() {
		return success;
	}
	
	public boolean getSuceess() {
		return success;
	}

	public void setSuceess(boolean suceess) {
		this.success = suceess;
	}
	
	@Override
	public String toString(){
		return String.format("NewOrModifyInstanceResponseMessage: primaryType=%d, sourceId=%d, streamName=%s, eqlId=%d, success=%s",
				primaryType, sourceId, streamName, eqlId, success?"true":"false");
	}
	
	@Override
	public int hashCode(){
		StringBuilder sb=new StringBuilder();
		sb.append(primaryType);
		sb.append(sourceId);
		sb.append(eqlId);
		sb.append(streamName);
		//sb.append(success);
		return sb.toString().hashCode();
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof NewOrModifyInstanceResponseMessage){
			NewOrModifyInstanceResponseMessage that=(NewOrModifyInstanceResponseMessage)obj;
			if(that.primaryType==this.primaryType &&
				that.eqlId==this.eqlId &&
				that.sourceId.equals(this.sourceId) &&
				that.streamName.equals(this.streamName)){
				return true;
			}
		}
		return false;
	}

	public static class NewOrModifyInstanceResponseMessageSerializer extends Serializer<NewOrModifyInstanceResponseMessage>{
		@Override
		public void write(Kryo kryo, Output output,
				NewOrModifyInstanceResponseMessage nmirm) {
			kryo.writeObject(output, nmirm.getPrimaryType());
			kryo.writeObject(output, nmirm.getSourceId());
			kryo.writeObject(output, nmirm.getEqlId());
			kryo.writeObject(output, nmirm.getStreamName());
			kryo.writeObject(output, nmirm.getSuceess());
		}

		@Override
		public NewOrModifyInstanceResponseMessage read(Kryo kryo, Input input,
				Class<NewOrModifyInstanceResponseMessage> type) {
			NewOrModifyInstanceResponseMessage nmirm=new NewOrModifyInstanceResponseMessage();
			nmirm.primaryType = kryo.readObject(input, Integer.class);
			nmirm.sourceId = kryo.readObject(input, String.class);
			nmirm.eqlId = kryo.readObject(input, Long.class);
			nmirm.streamName = kryo.readObject(input, String.class);
			nmirm.success = kryo.readObject(input, Boolean.class);
			return nmirm;
		}
	}
}
