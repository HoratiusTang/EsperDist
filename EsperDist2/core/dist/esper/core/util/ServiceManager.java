package dist.esper.core.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.configuration.*;
import org.apache.log4j.PropertyConfigurator;

import dist.esper.core.comm.LinkManager;
import dist.esper.core.comm.kryosocket.KryoSocketLinkManager;
import dist.esper.core.comm.simulation.SimulationLinkPool;
import dist.esper.core.id.WorkerId;
import dist.esper.event.EventRegistry;

public class ServiceManager {
	static Map<String,ServiceManager> serviceManagerMap=new HashMap<String,ServiceManager>();
	static final long DEFAULT_OUTPUT_INTERVAL_US=1*1000*1000;
	public static WorkerId coordinatorId;
	static boolean isSimulation=true;
	static Config config=null;
	
	LinkManager linkManager=null;
	EventRegistry eventRegistry=new EventRegistry();
	SortedMap<String,WorkerId> workerIdMap=new ConcurrentSkipListMap<String,WorkerId>();
	String myId;
	
	
	public static void initConfig(String[] args) throws ConfigurationException{
		if(config==null){
			config=new Config(args);
		}
		String log4jConfPath=config.get(Options.LOG4J_CONF_PATH);
		PropertyConfigurator.configure(log4jConfPath);
		
		String coordId=config.get(Options.COORDINATOR_ID);
		String coordIp=config.get(Options.COORDINATOR_IP);
		int coordPort=Integer.parseInt(config.get(Options.COORDINATOR_PORT));
		coordinatorId=new WorkerId(coordId, coordIp, coordPort);
	}
	
	public static Config getConfig(){
		return config;
	}
	
	public static ServiceManager getDefaultInstance(){
		return getInstance(config.get(Options.COORDINATOR_ID));
	}
	
	public static ServiceManager getInstance(String workerId){
		initInstanceIfNeeded(workerId);
		return serviceManagerMap.get(workerId);
	}
	
	public static void initInstanceIfNeeded(String workerId){
		if(!serviceManagerMap.containsKey(workerId)){
			ServiceManager sm=new ServiceManager(workerId);
			serviceManagerMap.put(workerId, sm);
		}
	}
	
	public static String getCoordinatorId() {
		return coordinatorId.getId();
	}
	
	public static WorkerId getCoordinatorWorkerId(){
		return coordinatorId;
	}
	
	public static boolean isSimulation() {
		return isSimulation;
	}

	public static void setSimulation(boolean isSimulation) {
		ServiceManager.isSimulation = isSimulation;
	}

	public ServiceManager(String workerId) {
		super();
		this.myId = workerId;
	}
	
	public Collection<WorkerId> getWorkerIds(){
		return workerIdMap.values();
	}

	public EventRegistry getEventRegistry(){
		return eventRegistry;
	}
	
	public static long getOutputIntervalUS() {		
		return config.getLong(Options.OUTPUT_INTERVERAL_US, DEFAULT_OUTPUT_INTERVAL_US);
	}
	
	public LinkManager getLinkManager(){
		WorkerId myId=getMyWorkerId();
		if(linkManager==null){
			if(isSimulation){
				linkManager=SimulationLinkPool.getInstance().newLinkManager(myId);
			}
			else{
				linkManager=new KryoSocketLinkManager(myId);
			}
		}
		return linkManager;
	}
	
	public WorkerId registerWorkerId(WorkerId workerId){
		return workerIdMap.put(workerId.getId(), workerId);
	}
	
	public SortedMap<String, WorkerId> getWorkerIdMap() {
		return workerIdMap;
	}

	public WorkerId getWorkerId(String workerId){
		return workerIdMap.get(workerId);
	}
	
	public WorkerId getMyWorkerId(){
		if(myId==coordinatorId.getId()){
			return coordinatorId;
		}
		else{
			return workerIdMap.get(myId);
		}
	}
	
	public static String getLocalhostIP() throws UnknownHostException{
		return InetAddress.getLocalHost().getHostAddress();
	}
}
