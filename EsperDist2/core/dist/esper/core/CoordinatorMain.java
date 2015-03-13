package dist.esper.core;

import java.util.List;

import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.experiment.util.MultiLineFileWriter;
import dist.esper.util.Logger2;

public class CoordinatorMain{
	static Logger2 log=Logger2.getLogger(CoordinatorMain.class);
	public static void main(String[] args){
		String coordId=null;
		Coordinator coord;
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			coordId=ServiceManager.getCoordinatorId();
			coord=new Coordinator(coordId);
			coord.init();
			new Thread(new SumbitQueryRunnable(coord)).start();
			coord.start();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			log.info("Coordinator terminated");
		}
	}
	
	/**
	 * read queries from file, and submit to Coordinator periodically
	 * @author tjy
	 */
	static class SumbitQueryRunnable implements Runnable{
		Coordinator coord;
		long intervalMS=5000;
		
		public SumbitQueryRunnable(Coordinator coord) {
			super();
			this.coord = coord;
		}

		@Override
		public void run() {
			String queriesFilePath=ServiceManager.getConfig().get(Options.QUERY_FILE);
			if(queriesFilePath==null){
				return;
			}
			try{
				Thread.sleep(15*1000);//waiting for workers and spouts start
				List<String> queryList=MultiLineFileWriter.readFromFile(queriesFilePath);
				log.info("read %d queries from %s", queryList.size(), queriesFilePath);
				log.info("begin submit queries");
				for(String query: queryList){
					coord.executeEPL(query);
					Thread.sleep(intervalMS);
				}
				log.info("end submit queries");
			}
			catch(Exception ex){
				log.error(ex.getMessage());
			}			
		}	
	}
}


