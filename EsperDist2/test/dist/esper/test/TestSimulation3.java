package dist.esper.test;

import java.util.*;

import org.apache.commons.configuration.ConfigurationException;

import dist.esper.core.CoordinatorMain;
import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.Worker;
import dist.esper.event.Event;
import dist.esper.event.EventRegistry;
import dist.esper.experiment.EventGeneratorFactory;
import dist.esper.experiment.QueryGeneratorMain;
import dist.esper.experiment.util.MultiLineFileWriter;
import dist.esper.external.Spout;
import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.external.event.FieldGenerator;
import dist.esper.external.event.FieldGeneratorFactory;
import dist.esper.io.GlobalStat;
import dist.esper.io.KryoFileWriter;
import dist.esper.util.Logger2;
import dist.esper.util.Tuple2D;

public class TestSimulation3 {
	static Logger2 log=Logger2.getLogger(TestSimulation3.class);
	//List<Event> eventList=new ArrayList<Event>();
	List<EventInstanceGenerator> eventGenList=new ArrayList<EventInstanceGenerator>(4);
	List<Worker> workerList=new ArrayList<Worker>();
	List<Spout> spoutList=new ArrayList<Spout>();
	Coordinator coord=null;
	
	public static void main(String[] args){
		TestSimulation3 sim=new TestSimulation3();
		try {
			sim.init(args);
			sim.run();
		}
		catch (Exception e) {			
			e.printStackTrace();
		}
	}
	
	public void init(String[] args) throws ConfigurationException{
		ServiceManager.initConfig(args);
		ServiceManager.coordinatorId=
			new WorkerId(ServiceManager.getCoordinatorId(),"127.0.0.1",1000);
		WorkerId[] workerIds=new WorkerId[]{
			new WorkerId("worker1","127.0.0.1",1001),
			new WorkerId("worker2","127.0.0.1",1002),
			new WorkerId("worker3","127.0.0.1",1003),
			new WorkerId("worker4","127.0.0.1",1004),
			new WorkerId("worker5","127.0.0.1",1005),
		};
		
		for(WorkerId wm: workerIds){
			ServiceManager.getInstance(wm.getId()).registerWorkerId(wm);//register itself
		}
		
		coord=new Coordinator(ServiceManager.getCoordinatorId());
		for(WorkerId wm: workerIds){
			Worker worker=new Worker(wm.getId());
			workerList.add(worker);
		}
		
		coord.init();
		for(Worker w: workerList){
			w.init();
		}
		
		eventGenList=QueryGeneratorMain.genEventInstanceGenerators();
		for(int i=0;i<eventGenList.size();i++){
			String id="spout"+(i+1);
			WorkerId spoutId=new WorkerId(id,"127.0.0.1",2001+i);
			spoutId.setSpout();
			ServiceManager.getInstance(spoutId.getId()).registerWorkerId(spoutId);//register itself
			
			Spout spout=new Spout(id, eventGenList.get(i));
			spoutList.add(spout);
			spout.init();
		}
	}
	
	public void run() throws Exception{
		String queriesFilePath="query/queries.txt";
		List<String> queryList=MultiLineFileWriter.readFromFile(queriesFilePath);
		log.info("read %d queries from %s", queryList.size(), queriesFilePath);
		
		coord.start(false);
		for(Spout spout: spoutList){
			spout.start(false);
		}
		for(Worker w: workerList){
			w.start(false);
		}		
		sleep(5000);

		int i=0;
		try {
			for(i=0; i<queryList.size();i++){
				String query=queryList.get(i);
				coord.executeEPL(query);
				sleep(5000);
				System.gc();
			}
			writeGlobalStat(coord);
			while(true){
				sleep(10000);
			}
			//System.out.print("");
		}
		catch (Exception e) {
			log.info("stopped at "+i);
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public static void writeGlobalStat(Coordinator coord){
		log.info("start writing to GlobalStat");
		String filePath="globalstat.bin";
		GlobalStat gs=new GlobalStat();
		gs.setEventMap(ServiceManager.getDefaultInstance().getEventRegistry().getEventMap());
		gs.setWorkerIdMap(ServiceManager.getDefaultInstance().getWorkerIdMap());
		gs.setRawStreamStatMap(coord.costEval.rawStats.getRawStreamStatMap());
		gs.setContainerNameMap(coord.costEval.containerNameMap);
		gs.setContainerStatMap(coord.costEval.containerStatMap);
		gs.setProcWorkerStatMap(coord.costEval.procWorkerStatMap);
		gs.setGateWorkerStatMap(coord.costEval.gateWorkerStatMap);
		gs.setContainerTreeMap(coord.containerTreeMap);
		gs.setContainerIdMap(coord.containerIdMap);
		
		KryoFileWriter.writeToFile(gs, filePath);
		log.info("finish writing to GlobalStat");
	}
	
	public static void sleep(long timeMS){
		try {Thread.sleep(timeMS);} catch(InterruptedException e1){e1.printStackTrace();}
	}	
}
