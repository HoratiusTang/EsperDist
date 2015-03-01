package dist.esper.core;

import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.Worker;
import dist.esper.util.Logger2;

public class WorkerMain {
	static Logger2 log=Logger2.getLogger(WorkerMain.class);
	public static void main(String[] args){
		String thisId=null;
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			thisId=regist();
			Worker worker=new Worker(thisId);
			worker.init();
			worker.start();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			log.info("Worker %s terminated", thisId);
		}
	}
	
	private static String regist() throws Exception{
		String thisId=ServiceManager.getConfig().get(Options.THIS_ID);
		thisId=thisId!=null?thisId:"Worker"+(int)(Math.random()*1000);
		String thisIp=ServiceManager.getLocalhostIP();
		int thisPort=Integer.parseInt(ServiceManager.getConfig().get(Options.THIS_PORT));		
		WorkerId myId=new WorkerId(thisId, thisIp, thisPort);
		
		System.out.println("construct WorkerId: "+myId.toString());
		ServiceManager.getInstance(thisId).registerWorkerId(myId);
		
		return thisId;
	}
}
