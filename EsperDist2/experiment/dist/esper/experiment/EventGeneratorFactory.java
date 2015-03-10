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
		return eventGen;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorA(){
		EventInstanceGenerator eventGen=new EventInstanceGenerator();
		eventGen.addProperty("id", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		eventGen.addProperty("age", new FieldGeneratorFactory.IntegerNormalGenerator(40, 1, 1, 100));
		eventGen.addProperty("name", new FieldGeneratorFactory.StringRandomChooser(new String[]{"Alice","Bob","Cleve","David","Elvis","Fedon","Glora","Harry","Illis"}));
		eventGen.addProperty("price", new FieldGeneratorFactory.DoubleUniformGenerator(10, 100));
		eventGen.addProperty("time", new FieldGeneratorFactory.LongNormalGenerator(10, 1, 1, 30));
		eventGen.addProperty("clientIds", new FieldGeneratorFactory.IntArrayGenerator(10));
		return eventGen;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorB(){
		EventInstanceGenerator eventGen=new EventInstanceGenerator();
		eventGen.addProperty("id", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		eventGen.addProperty("age", new FieldGeneratorFactory.IntegerNormalGenerator(40, 1, 1, 100));
		eventGen.addProperty("name", new FieldGeneratorFactory.StringRandomChooser(new String[]{"Alice","Bob","Cleve","David","Elvis","Fedon","Glora","Harry","Illis"}));
		eventGen.addProperty("price", new FieldGeneratorFactory.DoubleUniformGenerator(10, 100));
		eventGen.addProperty("time", new FieldGeneratorFactory.LongNormalGenerator(10, 1, 1, 30));
		eventGen.addProperty("clientIds", new FieldGeneratorFactory.IntArrayGenerator(10));
		return eventGen;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorC(){
		EventInstanceGenerator eventGen=new EventInstanceGenerator();
		eventGen.addProperty("id", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		eventGen.addProperty("age", new FieldGeneratorFactory.IntegerNormalGenerator(40, 1, 1, 100));
		eventGen.addProperty("name", new FieldGeneratorFactory.StringRandomChooser(new String[]{"Alice","Bob","Cleve","David","Elvis","Fedon","Glora","Harry","Illis"}));
		eventGen.addProperty("price", new FieldGeneratorFactory.DoubleUniformGenerator(10, 100));
		eventGen.addProperty("time", new FieldGeneratorFactory.LongNormalGenerator(10, 1, 1, 30));
		eventGen.addProperty("clientIds", new FieldGeneratorFactory.IntArrayGenerator(10));
		return eventGen;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorD(){
		EventInstanceGenerator eventGen=new EventInstanceGenerator();
		eventGen.addProperty("id", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		eventGen.addProperty("age", new FieldGeneratorFactory.IntegerNormalGenerator(40, 1, 1, 100));
		eventGen.addProperty("name", new FieldGeneratorFactory.StringRandomChooser(new String[]{"Alice","Bob","Cleve","David","Elvis","Fedon","Glora","Harry","Illis"}));
		eventGen.addProperty("price", new FieldGeneratorFactory.DoubleUniformGenerator(10, 100));
		eventGen.addProperty("time", new FieldGeneratorFactory.LongNormalGenerator(10, 1, 1, 30));
		eventGen.addProperty("clientIds", new FieldGeneratorFactory.IntArrayGenerator(10));
		return eventGen;
	}
	
	public static EventInstanceGenerator genEventInstanceGeneratorE(){
		EventInstanceGenerator eventGen=new EventInstanceGenerator();
		eventGen.addProperty("id", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		eventGen.addProperty("age", new FieldGeneratorFactory.IntegerNormalGenerator(40, 1, 1, 100));
		eventGen.addProperty("name", new FieldGeneratorFactory.StringRandomChooser(new String[]{"Alice","Bob","Cleve","David","Elvis","Fedon","Glora","Harry","Illis"}));
		eventGen.addProperty("price", new FieldGeneratorFactory.DoubleUniformGenerator(10, 100));
		eventGen.addProperty("time", new FieldGeneratorFactory.LongNormalGenerator(10, 1, 1, 30));
		eventGen.addProperty("clientIds", new FieldGeneratorFactory.IntArrayGenerator(10));
		return eventGen;
	}
}
