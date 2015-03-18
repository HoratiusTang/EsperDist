package combine.esper.core;

import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import combine.esper.core.worker.Worker;
import dist.esper.io.KryoClassRegister;
import dist.esper.util.Logger2;

public class WorkerMain {
	static Logger2 log=Logger2.getLogger(WorkerMain.class);
	public static void main(String[] args){
		String thisId=null;
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			KryoClassRegister.addPackageName("combine.esper.core.message");
			KryoClassRegister.addPackageName("combine.esper.core.cost");
			thisId=regist();
			Worker worker=new Worker(thisId);
			worker.init();
			worker.start();
		}
		catch (Exception e) {
			log.getLogger().fatal("fatal error ocurr", e);
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
		
		log.info("construct WorkerId: "+myId.toString());
		ServiceManager.getInstance(thisId).registerWorkerId(myId);
		
		return thisId;
	}
}
