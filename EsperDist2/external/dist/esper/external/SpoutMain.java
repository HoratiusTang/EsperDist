package dist.esper.external;

import java.util.HashMap;
import java.util.Map;

import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.event.Event;
import dist.esper.experiment.EventGeneratorFactory;
import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.external.event.FieldGenerator;
import dist.esper.external.event.FieldGeneratorFactory;

public class SpoutMain {
	public static void main(String[] args){
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			String thisId=regist();			
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
	
	private static EventInstanceGenerator genEventInstanceGenerator() throws Exception{
		String eventCategory=ServiceManager.getConfig().get(Options.EVENT_CATEGORY);
		String eventName=ServiceManager.getConfig().get(Options.EVENT_NAME);
		if(eventCategory==null){
			throw new Exception(String.format("not found option '%s'", Options.EVENT_CATEGORY));
		}
		if(eventName==null){
			throw new Exception(String.format("not found option '%s'", Options.EVENT_NAME));
		}		
		return EventGeneratorFactory.genEventInstanceGenerator(eventCategory, eventName);
	}
}
