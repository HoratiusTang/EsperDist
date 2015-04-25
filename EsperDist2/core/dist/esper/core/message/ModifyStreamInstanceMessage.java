package dist.esper.core.message;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import dist.esper.core.flow.container.StreamContainer;

@DefaultSerializer(value = ModifyStreamInstanceMessage.ModifyStreamInstanceMessageSerializer.class)
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
	
	public static class ModifyStreamInstanceMessageSerializer extends Serializer<ModifyStreamInstanceMessage>{
		@Override
		public void write(Kryo kryo, Output output,
				ModifyStreamInstanceMessage msim) {
			kryo.writeObject(output, msim.getPrimaryType());
			kryo.writeObject(output, msim.getSourceId());
			StreamContainer.streamContainersLock.lock();
			kryo.writeClassAndObject(output, msim.getStreamContainer());
			StreamContainer.streamContainersLock.unlock();
		}

		@Override
		public ModifyStreamInstanceMessage read(Kryo kryo, Input input,
				Class<ModifyStreamInstanceMessage> type) {
			ModifyStreamInstanceMessage msim=new ModifyStreamInstanceMessage();
			msim.primaryType = kryo.readObject(input, Integer.class);
			msim.sourceId = kryo.readObject(input, String.class);			
			msim.streamContainer = (StreamContainer) kryo.readClassAndObject(input);
			return msim;
		}
	}
	
}
