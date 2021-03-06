package dist.esper.core.message;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.flow.container.StreamContainer;

@DefaultSerializer(value = NewStreamInstanceMessage.NewStreamInstanceMessageSerializer.class)
public class NewStreamInstanceMessage extends AbstractMessage{
	private static final long serialVersionUID = -7798239742265480819L;
	StreamContainer streamContainer;
	long eqlId;
	
	public NewStreamInstanceMessage() {
		super();
	}

	public NewStreamInstanceMessage(String sourceId) {
		super(sourceId);
		this.primaryType=PrimaryTypes.CONTROLL;
	}
	
	public NewStreamInstanceMessage(String sourceId, StreamContainer streamContainer, long eqlId) {
		this(sourceId);
		this.sourceId = sourceId;
		this.eqlId = eqlId;
		this.streamContainer = streamContainer;
	}

	public StreamContainer getStreamContainer() {
		return streamContainer;
	}

	public void setStreamContainer(StreamContainer streamContainer) {
		this.streamContainer = streamContainer;
	}	
	
	public long getEqlId() {
		return eqlId;
	}

	public void setEqlId(long eqlId) {
		this.eqlId = eqlId;
	}


	public static class NewStreamInstanceMessageSerializer extends Serializer<NewStreamInstanceMessage>{
		@Override
		public void write(Kryo kryo, Output output,
				NewStreamInstanceMessage nsim) {
			kryo.writeObject(output, nsim.getPrimaryType());
			kryo.writeObject(output, nsim.getSourceId());
			kryo.writeObject(output, nsim.getEqlId());
			kryo.writeClassAndObject(output, nsim.getStreamContainer());			
		}

		@Override
		public NewStreamInstanceMessage read(Kryo kryo, Input input,
				Class<NewStreamInstanceMessage> type) {
			NewStreamInstanceMessage nsim=new NewStreamInstanceMessage();
			nsim.primaryType = kryo.readObject(input, Integer.class);
			nsim.sourceId = kryo.readObject(input, String.class);
			nsim.eqlId = kryo.readObject(input, Long.class);					
			nsim.streamContainer = (StreamContainer) kryo.readClassAndObject(input);
			return nsim;
		}
	}
}