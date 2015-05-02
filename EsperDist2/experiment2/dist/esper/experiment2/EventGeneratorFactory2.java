package dist.esper.experiment2;

import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.external.event.FieldGeneratorFactory;

public class EventGeneratorFactory2 {
	public static EventInstanceGenerator[] genEventInstanceGenerators(String[] eventNames){
		EventInstanceGenerator[] eigs=new EventInstanceGenerator[eventNames.length];
		for(int i=0; i<eventNames.length; i++){
			eigs[i]=genEventInstanceGeneratorDefault(eventNames[i]);
		}
		return eigs;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorDefault(String eventName){
		EventInstanceGenerator eventGen=new EventInstanceGenerator("DEFAULT_PROTOTYPE", eventName);
		//eventGen.addProperty("im0", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		eventGen.addProperty("in0", new FieldGeneratorFactory.IntegerNormalGenerator(100, 10, 0, 200));
		eventGen.addProperty("in1", new FieldGeneratorFactory.IntegerNormalGenerator(200, 20, 0, 400));		
		//eventGen.addProperty("in2", new FieldGeneratorFactory.IntegerNormalGenerator(300, 30, 0, 600));
		//eventGen.addProperty("in3", new FieldGeneratorFactory.IntegerNormalGenerator(400, 40, 0, 800));
		//eventGen.addProperty("in4", new FieldGeneratorFactory.IntegerNormalGenerator(500, 50, 0, 1000));
		eventGen.addProperty("iu0", new FieldGeneratorFactory.IntegerUniformGenerator(100, 500));
		eventGen.addProperty("iu1", new FieldGeneratorFactory.IntegerUniformGenerator(0, 600));
		eventGen.addProperty("dn0", new FieldGeneratorFactory.DoubleNormalGenerator(200, 20, 0, 400));
		eventGen.addProperty("dn1", new FieldGeneratorFactory.DoubleNormalGenerator(300, 30, 0, 600));
		eventGen.addProperty("du0", new FieldGeneratorFactory.DoubleUniformGenerator(200, 300));
		eventGen.addProperty("du1", new FieldGeneratorFactory.DoubleUniformGenerator(300, 800));
		//eventGen.addProperty("sr0", new FieldGeneratorFactory.StringRandomChooser(genRandomStrings(100)));		
		//eventGen.addProperty("ia0", new FieldGeneratorFactory.IntArrayGenerator(10));
		return eventGen;
	}
	
	public static String[] genRandomStrings(int size){
		String[] s=new String[size];
		for(int i=0;i<s.length;i++){
			s[i]="randomstring"+String.format("%04d", i);
		}
		return s;
	}
}
