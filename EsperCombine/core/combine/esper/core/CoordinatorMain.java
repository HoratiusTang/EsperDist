package combine.esper.core;

import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.*;
import dist.esper.util.Logger2;

public class CoordinatorMain{
	static Logger2 log=Logger2.getLogger(CoordinatorMain.class);
	public static void main(String[] args){
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			KryoClassRegister.addPackageName("combine.esper.core.message");
			String coordId=ServiceManager.getCoordinatorId();
			Coordinator coord=new Coordinator(coordId);
			coord.init();
			coord.start();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			System.out.println("Coordinator terminated");
		}
	}
}
