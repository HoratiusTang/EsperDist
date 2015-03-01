package dist.esper.core;

import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.util.ServiceManager;
import dist.esper.util.Logger2;

public class CoordinatorMain{
	static Logger2 log=Logger2.getLogger(CoordinatorMain.class);
	public static void main(String[] args){
		String coordId=null;
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			coordId=ServiceManager.getCoordinatorId();
			Coordinator coord=new Coordinator(coordId);
			coord.init();
			coord.start();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			log.info("Coordinator terminated");
		}
	}
}
