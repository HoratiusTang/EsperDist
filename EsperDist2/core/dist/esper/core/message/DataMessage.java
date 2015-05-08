package dist.esper.core.message;

import java.util.Map;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

import dist.esper.core.coordinator.CoordinatorStatReportor;
import dist.esper.util.Logger2;

@DefaultSerializer(value = DataMessage.DataMessageSerializer.class)
public class DataMessage extends AbstractMessage{
	private static final long serialVersionUID = -9095742802503610806L;
	String streamName;
	String[] elementNames;
	Object[] data;
	
	public DataMessage() {
		super();
		this.primaryType = PrimaryTypes.DATA;
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
	
	public static class DataMessageSerializer extends Serializer<DataMessage>{
		static Logger2 log=Logger2.getLogger(DataMessageSerializer.class);
		static Integer RAW_EVENT=Integer.valueOf(0x1010);
		static Integer RECORD=Integer.valueOf(0x0101);
		@Override
		public void write(Kryo kryo, Output output, DataMessage dm) {
			kryo.writeObject(output, dm.getPrimaryType());
			kryo.writeObject(output, dm.getSourceId());
			kryo.writeObject(output, dm.getStreamName());
			kryo.writeObject(output, dm.getElementNames());
			Object[] events=dm.getData();
			kryo.writeObject(output, events.length);
			
			if(events[0] instanceof Map<?,?>){
				kryo.writeObject(output, RAW_EVENT);
				kryo.writeClass(output, events[0].getClass());
				for(int i=0; i<events.length; i++){
					kryo.writeObject(output, events[i]);
				}				
			}
			else{
				kryo.writeObject(output, RECORD);
				Object[][] records=(Object[][])events;
				kryo.writeObject(output, records[0].length);
				Serializer<?>[] sers=new Serializer<?>[records[0].length];
				for(int j=0; j<records[0].length; j++){
					Registration reg=kryo.writeClass(output, records[0][j].getClass());
					sers[j]=reg.getSerializer();
				}
				for(int i=0; i<records.length; i++){
					for(int j=0; j<records[i].length; j++){
						kryo.writeObject(output, records[i][j], sers[j]);
					}
				}
			}
		}

		@Override
		public DataMessage read(Kryo kryo, Input input, Class<DataMessage> type) {
			DataMessage dm=new DataMessage();
			dm.primaryType = kryo.readObject(input, Integer.class);
			dm.sourceId = kryo.readObject(input, String.class);
			dm.streamName = kryo.readObject(input, String.class);
			dm.elementNames = kryo.readObject(input, String[].class);
			
			int eventCount=kryo.readObject(input, Integer.class);
			int category = kryo.readObject(input, Integer.class);
			
			if(category==RAW_EVENT){
				Registration reg=kryo.readClass(input);
				Object[] events=new Object[eventCount];
				for(int i=0; i<eventCount; i++){					
					events[i]=kryo.readObject(input, reg.getType(), reg.getSerializer());
				}
				dm.setData(events);
			}
			else if(category==RECORD){
				int colCount=kryo.readObject(input, Integer.class);
				Registration[] regs=new Registration[colCount];
				for(int j=0; j<colCount; j++){
					regs[j]=kryo.readClass(input);
				}
				
				Object[][] records=new Object[eventCount][];
				for(int i=0; i<eventCount; i++){
					records[i]=new Object[colCount];
					for(int j=0; j<colCount; j++){
						records[i][j]=kryo.readObject(input, regs[j].getType(), regs[j].getSerializer());
					}
				}
				dm.setData(records);
			}
			else{
				log.error("category is not RAW_EVENT or RECORD, category="+category);
				Object[][] records=new Object[0][];
				dm.setData(records);
			}	
			return dm;
		}
		
	}
}
