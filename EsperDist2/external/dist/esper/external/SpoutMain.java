package dist.esper.external;

import java.util.HashMap;
import java.util.Map;

import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.event.Event;
import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.external.event.FieldGenerator;
import dist.esper.external.event.FieldGeneratorFactory;

public class SpoutMain {
	public static void main(String[] args){
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			String thisId=regist();
			//Event event=genEvent();
			EventInstanceGenerator eventGen=genEventInstanceGenerator();
			Spout spout=new Spout(thisId, eventGen);//FIXME
			spout.init();
			spout.start(true);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			System.out.println("Spout terminated");
		}
	}
	
	private static String regist() throws Exception{
		String thisId=ServiceManager.getConfig().get(Options.THIS_ID);
		thisId=thisId!=null?thisId:"Spout"+(int)(Math.random()*1000);
		String thisIp=ServiceManager.getLocalhostIP();
		int thisPort=Integer.parseInt(ServiceManager.getConfig().get(Options.THIS_PORT));		
		WorkerId myId=new WorkerId(thisId, thisIp, thisPort);
		
		System.out.println("construct WorkerId: "+myId.toString());
		ServiceManager.getInstance(thisId).registerWorkerId(myId);
		
		return thisId;
	}
	
	/**
	private static Event genEvent() throws Exception{
		String eventName=ServiceManager.getConfig().get(Options.EVENT_NAME);
		if(eventName==null){
			throw new Exception(String.format("not found option '%s'", Options.EVENT_NAME));
		}
		Event e=new Event(eventName);
		e.addProperty("id", Integer.class);
		e.addProperty("age", Integer.class);
		e.addProperty("name", String.class);
		e.addProperty("price", Double.class);
		e.addProperty("time", Long.class);
		e.addProperty("clientIds", int[].class);
		return e;
	}*/
	
	private static EventInstanceGenerator genEventInstanceGenerator() throws Exception{
		String eventName=ServiceManager.getConfig().get(Options.EVENT_NAME);
		if(eventName==null){
			throw new Exception(String.format("not found option '%s'", Options.EVENT_NAME));
		}
		Event e=new Event(eventName);
		e.addProperty("id", Integer.class);
		e.addProperty("age", Integer.class);
		e.addProperty("name", String.class);
		e.addProperty("price", Double.class);
		e.addProperty("time", Long.class);
		e.addProperty("clientIds", int[].class);
		
		Map<String,FieldGenerator> fgMap=new HashMap<String,FieldGenerator>();
		fgMap.put("id", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		fgMap.put("age", new FieldGeneratorFactory.IntegerNormalGenerator(40, 1, 1, 100));
		fgMap.put("name", new FieldGeneratorFactory.StringRandomChooser(new String[]{"Alice","Bob","Cleve","David","Elvis","Fedon","Glora","Harry","Illis"}));
		fgMap.put("price", new FieldGeneratorFactory.DoubleUniformGenerator(10, 100));
		fgMap.put("time", new FieldGeneratorFactory.LongNormalGenerator(10, 1, 1, 30));
		fgMap.put("clientIds", new FieldGeneratorFactory.IntArrayGenerator(10));
		
		EventInstanceGenerator eventGen=new EventInstanceGenerator(e, fgMap);
		return eventGen;
	}
}
