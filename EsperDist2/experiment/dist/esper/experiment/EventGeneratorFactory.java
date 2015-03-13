package dist.esper.experiment;

import java.util.HashMap;
import java.util.Map;

import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.event.Event;
import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.external.event.FieldGenerator;
import dist.esper.external.event.FieldGeneratorFactory;

public class EventGeneratorFactory {
	public static EventInstanceGenerator genEventInstanceGenerator(String eventCategory, String eventName){
		EventInstanceGenerator eventGen=genEventInstanceGeneratorByCategory(eventCategory);
		eventGen.setEventName(eventName);
		return eventGen;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorByCategory(String eventCategory){
		EventInstanceGenerator eventGen=null;
		if(eventCategory.equals("A")){
			eventGen=genEventInstanceGeneratorA();
		}
		else if(eventCategory.equals("B")){
			eventGen=genEventInstanceGeneratorB();
		}
		else if(eventCategory.equals("C")){
			eventGen=genEventInstanceGeneratorC();
		}
		else if(eventCategory.equals("D")){
			eventGen=genEventInstanceGeneratorD();
		}
		else if(eventCategory.equals("E")){
			eventGen=genEventInstanceGeneratorE();
		}
		else{
			throw new RuntimeException(String.format("no such event category %s", eventCategory));
		}
		return eventGen;
	}
	
	public static String[] genRandomStrings(int size){
		String[] s=new String[size];
		for(int i=0;i<s.length;i++){
			s[i]="randomstring"+String.format("%04d", i);
		}
		return s;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorDefault(String categoryName){
		EventInstanceGenerator eventGen=new EventInstanceGenerator(categoryName);
		eventGen.addProperty("im0", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		eventGen.addProperty("in0", new FieldGeneratorFactory.IntegerNormalGenerator(100, 10, 0, 200));
		eventGen.addProperty("in1", new FieldGeneratorFactory.IntegerNormalGenerator(200, 20, 0, 400));		
		eventGen.addProperty("in2", new FieldGeneratorFactory.IntegerNormalGenerator(300, 30, 0, 600));
		eventGen.addProperty("in3", new FieldGeneratorFactory.IntegerNormalGenerator(400, 40, 0, 800));
		//eventGen.addProperty("in4", new FieldGeneratorFactory.IntegerNormalGenerator(500, 50, 0, 1000));
		eventGen.addProperty("iu0", new FieldGeneratorFactory.IntegerUniformGenerator(100, 500));
		eventGen.addProperty("iu1", new FieldGeneratorFactory.IntegerUniformGenerator(0, 600));
		eventGen.addProperty("dn0", new FieldGeneratorFactory.DoubleNormalGenerator(200, 20, 0, 400));
		//eventGen.addProperty("dn1", new FieldGeneratorFactory.DoubleNormalGenerator(300,30, 0, 600));
		eventGen.addProperty("du0", new FieldGeneratorFactory.DoubleUniformGenerator(200, 300));
		//eventGen.addProperty("du1", new FieldGeneratorFactory.DoubleUniformGenerator(300, 800));
		eventGen.addProperty("sr0", new FieldGeneratorFactory.StringRandomChooser(genRandomStrings(100)));		
		eventGen.addProperty("ia0", new FieldGeneratorFactory.IntArrayGenerator(10));
		return eventGen;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorA(){
		return genEventInstanceGeneratorDefault("A");
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorB(){
		return genEventInstanceGeneratorDefault("B");
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorC(){
		return genEventInstanceGeneratorDefault("C");
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorD(){
		return genEventInstanceGeneratorDefault("D");
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorE(){
		return genEventInstanceGeneratorDefault("E");
	}
}
