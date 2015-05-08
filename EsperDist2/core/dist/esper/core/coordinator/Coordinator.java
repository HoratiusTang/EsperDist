package dist.esper.core.coordinator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import com.espertech.esper.epl.spec.StatementSpecCompiled;

import dist.esper.core.comm.*;
import dist.esper.core.cost.*;
import dist.esper.core.cost.DeltaResourceUsage.CandidateContainerType;
import dist.esper.core.flow.centralized.*;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.container.DerivedStreamContainer.StreamAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.*;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResult;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.*;
import dist.esper.core.util.*;
import dist.esper.core.worker.pubsub.Processor;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.epl.sementic.StatementVisitor;
import dist.esper.event.Event;
import dist.esper.io.GlobalStat;
import dist.esper.proxy.EPAdministratorImplProxy;
import dist.esper.util.AsyncFileWriter;
import dist.esper.util.AsyncLogger2;
import dist.esper.util.BytesFileSerializer;
import dist.esper.util.CollectionUtils;
import dist.esper.util.DateUtil;
import dist.esper.util.Logger2;
import dist.esper.util.ThreadUtil;

public class Coordinator {
	static Logger2 log;
	//static Logger2 workerStatLog;
	static{
		log=Logger2.getLogger(Coordinator.class);
		//workerStatLog=AsyncLogger2.getAsyncLogger(Coordinator.class, Level.DEBUG, "log/workerstats.txt", false, "%d{MM-dd HH:mm:ss} %p %m%n");
	}
	public static final double GATEWAY_WORKER_RATIO_MIN=1.0d/3.0d;
	static int CENTRALIZED_TREE_PRUNE_COUNT=5;
	static double OVERLOAD_RATIO_THRESHOLD=0.5d;
	
	public String id;
	LinkManager linkManager;
	public EPServiceProvider epService;
	EPAdministratorImplProxy epAdminProxy;
	CentralizedTreeBuilder centralizedTreeBuilder;
	StreamFlowBuilder streamFlowBuilder;
	StreamContainerFlowBuilder streamContainerFlowBuilder;
	StreamReviewer streamReviewer;
	
	public List<FilterStreamContainer> existedFscList=new ArrayList<FilterStreamContainer>();
	public List<PatternStreamContainer> existedPscList=new ArrayList<PatternStreamContainer>();
	public List<JoinStreamContainer> existedJscList=new ArrayList<JoinStreamContainer>();
	public List<RootStreamContainer> existedRscList=new ArrayList<RootStreamContainer>();
	Map<String, DerivedStreamContainer> containerNameMap=new ConcurrentSkipListMap<String, DerivedStreamContainer>();
	public Map<Long, DerivedStreamContainer> containerIdMap=new ConcurrentSkipListMap<Long, DerivedStreamContainer>();
	public Map<Long, StreamContainerFlow> containerTreeMap=new ConcurrentSkipListMap<Long, StreamContainerFlow>();
	
	Map<String,Link> workerLinkMap=new ConcurrentSkipListMap<String,Link>();
	Map<String,Link> spoutLinkMap=new ConcurrentSkipListMap<String,Link>();
	Map<String,Link> monitorLinkMap=new ConcurrentSkipListMap<String,Link>();
	
	List<RawStream> rawStreamList=new ArrayList<RawStream>();
	int expectedSpoutCount=0;
	AtomicLong selectElementUID=new AtomicLong(0L);
	AtomicLong streamUID=new AtomicLong(0L);
	AtomicLong eplUID=new AtomicLong(0L);
	NewLinkHandler newLinkHandler=new NewLinkHandler();
	List<WorkerId> procWorkerIdList=new ArrayList<WorkerId>(8);
	List<WorkerId> gateWorkerIdList=new ArrayList<WorkerId>(4);
	public CostEvaluator costEval;
	
	Map<String,String> rawSamplingWorkerMap=new HashMap<String,String>();
	WorkerAssignmentStrategy rawSamplingStrategy=new WorkerAssignmentStrategy();
	
	CoordinatorStatReportor coordStatReportor;
	ReentrantLock containerMapLock=new ReentrantLock();
	
	MessageHandlingScheduler messageHandlingScheduler;
	MessageHandler messageHandler=new MessageHandler();
	
	MessageHandlingScheduler responseMessageHandlingScheduler;
	ResponseMessageHandler responseMessageHandler=new ResponseMessageHandler();
	
	AsyncFileWriter workerStatWriter;
	DelayedStreamContainerFlowRegistry delayedContainerFlowRegistry;	

	class NewLinkHandler implements LinkManager.NewLinkListener, Link.Listener{
		@Override public void connected(Link link) {}
		@Override 
		public void disconnected(Link link) {
			log.info("Coordinator's Link is disconnected: %s", link.toString());
			if(link.getTargetId().getType()==WorkerId.SPOUT)
				spoutLinkMap.remove(link.getTargetId().getId());
			else if(link.getTargetId().getType()==WorkerId.MONITOR)
				monitorLinkMap.remove(link.getTargetId().getId());
			else
				workerLinkMap.remove(link.getTargetId().getId());
		}
		
		@Override
		public void newReceivedLink(Link link) {
			//workerLinkMap.put(link.getTargetMeta().getId(), link);
			link.addListener(this);
			log.info("Coordinator %s accept link(%s) from %s", id, link.getLinkId(), link.getTargetId().getId());
		}

		@Override
		public void received(Link link, Object obj) {
			//handleReceiving(link, obj);
			if(obj instanceof AbstractMessage){
				AbstractMessage msg=(AbstractMessage)obj;
				if(msg.getPrimaryType()==PrimaryTypes.RESPONSE){
					responseMessageHandlingScheduler.submit(link, obj, responseMessageHandler);
				}
				else{
					messageHandlingScheduler.submit(link, obj, messageHandler);
				}
			}
			else{
				messageHandlingScheduler.submit(link, obj, messageHandler);
			}
		}
	}
	
	class MessageHandler implements MessageHandlingScheduler.IMessageHandler{
		@Override
		public void handleMessage(Link link, Object obj) {
			handleReceiving(link, obj);
		}
	}
	
	class ResponseMessageHandler implements MessageHandlingScheduler.IMessageHandler{
		ConcurrentHashMap<Object, Object> waitingMap=new ConcurrentHashMap<Object, Object>();
		Object dummy=new Object();
		@Override
		public void handleMessage(Link link, Object obj) {
			log.debug("ResponseMessageHandler recevied message: "+obj.toString());
			Semaphore sem=(Semaphore)waitingMap.putIfAbsent(obj, dummy);
			if(sem!=null){				
				sem.release();
				log.debug("ResponseMessageHandler released semaphore for "+obj.toString());
			}
			else{
				log.debug("ResponseMessageHandler added semaphore for "+obj.toString());
				return;//responsed before waitResponse()
			}
		}
		
		public void waitResponse(Object expectedResponse){
			Semaphore sem=new Semaphore(0);
			if(waitingMap.putIfAbsent(expectedResponse, sem) != null){//already responsed before waitResponse()
				waitingMap.remove(expectedResponse);
				log.debug("ResponseMessageHandler remove dummy for "+expectedResponse.toString());
			}
			else{
				try {
					log.debug("ResponseMessageHandler waiting for semaphore for "+expectedResponse.toString());
					sem.acquire(1);
					log.debug("ResponseMessageHandler waited for semaphore for "+expectedResponse.toString());
				}
				catch (InterruptedException e) {
					log.debug("error occur in waitResponse()", e);
				}
			}
		}
	}
	
	public void handleReceiving(Link link, Object obj){
		if(obj instanceof NewSpoutMessage){
			NewSpoutMessage nsm=(NewSpoutMessage)obj;
			WorkerId spoutId=link.getTargetId();
			spoutLinkMap.put(spoutId.getId(), link);
			
			RawStream rsl=new RawStream(spoutId, nsm.getEvent());
			this.registEPEvent(nsm.getEvent());
			this.registerRawStream(rsl);
			log.debug("%s regist new %s with Event %s", id, spoutId.toString(), nsm.getEvent().getName());
		}
		else if(obj instanceof NewWorkerMessage){
			WorkerId workerId=link.getTargetId();
			workerLinkMap.put(workerId.getId(), link);
			registerWorkerId(workerId);
			log.debug("%s regist new %s", id, workerId.toString());
		}
		else if(obj instanceof NewMonitorMessage){
			WorkerId monId=link.getTargetId();
			monitorLinkMap.put(monId.getId(), link);
			log.debug("%s regist new %s", id, monId.toString());
		}
		else if(obj instanceof SubmitEplRequest){
			SubmitEplRequest serq=(SubmitEplRequest)obj;
			log.debug("%s received epl %s with tag %d", id, serq.getEpl(), serq.getTag());
			SubmitEplResponse serp=null;
			try {
				long eplId=this.executeEPL(serq.getEpl());
				serp=new SubmitEplResponse(id, serq.getTag(), eplId, null);
			}
			catch (Exception e) {
				serp=new SubmitEplResponse(id, serq.getTag(), -1, e.getMessage());
				e.printStackTrace();
			}
			link.send(serp);
		}
		else if(obj instanceof WorkerStat){
			WorkerStat ws=(WorkerStat)obj;
			//System.err.println(ws.toString());
			delayedContainerFlowRegistry.checkContainerAppeared(ws.getInsStats());
			costEval.updateWorkerStat(ws);
			logWorkerStat(ws);
		}
	}
	
	private void logWorkerStat(WorkerStat ws){
		String str=String.format("WorkerId=%s, isGateway=%s, memUsed=%d, memFree=%d, " +
				"bwUsageUs=%.2f, cpuUsage=%.2f, sendByteRateUS=%.2f, sendBaseTimeUS=%.2f, " +
				"procThreadCount=%d, pubThreadCount=%d, localSubscriberCount=%d, " +
				"remoteSubscriberCount=%d, localPublisherCount=%d, remotePublisherCount=%d" +
				"filterCondProcTimeUS=%.2f, joinCondProcTimeUS=%.2f, " +
				"filterCount=%d, filterDelayedCount=%d, " +
				"joinCount=%d, joinDelayedCount=%d, " +
				"rootCount=%d, rawStreamSampleCount=%d\n", 
				ws.id, ws.isGateway, ws.memUsed, 
				ws.memFree, ws.bwUsageUS, ws.cpuUsage,
				ws.sendByteRateUS, ws.sendBaseTimeUS,
				ws.procThreadCount, ws.pubThreadCount,
				ws.localSubscriberCount, ws.remoteSubscriberCount,
				ws.localPublisherCount, ws.remotePublisherCount,
				ws.filterCondProcTimeUS, ws.joinCondProcTimeUS,
				ws.filterCount, ws.filterDelayedCount,
				ws.joinCount, ws.joinDelayedCount,
				ws.rootCount, ws.getRawStreamSampleCount());
		workerStatWriter.append(str);
	}
	
	public Coordinator(String id){
		this.id=id;
	}
	
	public void init(){
		epService = EPServiceProviderManager.getProvider(id);
		epAdminProxy = new EPAdministratorImplProxy(epService.getEPAdministrator());
		costEval=new CostEvaluator(containerNameMap);
		
		messageHandlingScheduler=new MessageHandlingScheduler(id, 1);
		responseMessageHandlingScheduler=new MessageHandlingScheduler(id, 1);
		
		workerStatWriter=new AsyncFileWriter("log/workerstats", 8000);
		
		linkManager=ServiceManager.getInstance(id).getLinkManager();
		linkManager.init();
		linkManager.setNewLinkListener(newLinkHandler);
		
		delayedContainerFlowRegistry=new DelayedStreamContainerFlowRegistry(this);
		centralizedTreeBuilder=new CentralizedTreeBuilder(this);
		streamFlowBuilder=new StreamFlowBuilder(this);
		streamContainerFlowBuilder=new StreamContainerFlowBuilder(this);
		coordStatReportor=new CoordinatorStatReportor(this);
		streamReviewer=new StreamReviewer(existedFscList, existedPscList,
				existedJscList, existedRscList);
		expectedSpoutCount=(int)ServiceManager.getConfig().getLong(Options.EXPECTED_SPOUNT_COUNT, 0);
	}
	
	public void start(){
		start(true);
	}
	public void start(boolean sync){
		log.info("Coordinator is running...");
		if(sync){
			coordStatReportor.run();
		}
		else{
			new Thread(coordStatReportor).start();
		}
	}
	
	public void registEPEvent(Event event){
		ServiceManager.getInstance(id).getEventRegistry().registEvent(event);
		epService.getEPAdministrator().getConfiguration().addEventType(event.getName(), event);
	}
	
	public long executeEPL(String epl) throws Exception{
		double overloadRatio=costEval.getOverloadedWorkerRatio();
		if(overloadRatio>OVERLOAD_RATIO_THRESHOLD){
			log.error("System can NOT execute new EPL, for %.2f%% workers are overloaded: %s", 
					overloadRatio*100, costEval.getAllWorkerStatsDesciption());
			ThreadUtil.sleep(1000);
			return -1;
		}
		if(rawStreamList.size() < expectedSpoutCount){
			log.info("System is waiting for %d Spouts to connect, but there is only %d now", expectedSpoutCount, rawStreamList.size());
			return -1;
		}
		long eplId=eplUID.getAndIncrement();
		List<Tree> treeList=centralizedTreeBuilder.generateTree(eplId, epl);
		if(treeList.size()>CENTRALIZED_TREE_PRUNE_COUNT){
			treeList=treeList.subList(0, CENTRALIZED_TREE_PRUNE_COUNT);
		}
		List<StreamFlow> sfList=new ArrayList<StreamFlow>(treeList.size());
		List<DeltaResourceUsage> druList=new ArrayList<DeltaResourceUsage>();		
		for(Tree tree: treeList){
			StreamFlow sf=streamFlowBuilder.buildStreamFlow(tree);
			sfList.add(sf);
		}		
		for(StreamFlow sf: sfList){
			DeltaResourceUsage dru=this.computeBestDeltaResourceUsage(sf);
			druList.add(dru);
		}		
		int index=costEval.chooseBestIndex(druList);
		StreamContainer.streamContainersLock.readLock().lock();
		StreamContainerFlow sct=streamContainerFlowBuilder.buildStreamContainerFlow(sfList.get(index), druList.get(index));
		this.submit(sct);
		StreamContainer.streamContainersLock.readLock().unlock();
		return eplId;
	}
	
	public DeltaResourceUsage computeBestDeltaResourceUsage(StreamFlow sf){
		streamReviewer.reset(sf.getRootStream());
		streamReviewer.check();
		DeltaResourceUsage rootDRU=costEval.computeBestStrategy(sf.getRootStream());
		return rootDRU;
	}
	
	
	public List<RawStream> getRawStreamList() {
		return rawStreamList;
	}
	
	public void registerWorkerId(WorkerId newWM){
		ServiceManager.getInstance(id).registerWorkerId(newWM);
		int index1=-1;
		for(int i=0;i<procWorkerIdList.size();i++){
			if(newWM.getId().equals(procWorkerIdList.get(i).getId())){
				index1=i;
				break;
			}
		}
		if(index1>=0)
			procWorkerIdList.set(index1, newWM);
		
		int index2=-1;
		for(int i=0;i<gateWorkerIdList.size();i++){
			if(newWM.getId().equals(gateWorkerIdList.get(i).getId())){
				index2=i;
				break;
			}
		}
		if(index2>=0)
			gateWorkerIdList.set(index2, newWM);
		
		if(index1<0 && index2<0){
			/*determine processing-worker or gate-worker*/
			if(gateWorkerIdList.size()==0 || 
					(procWorkerIdList.size()>0 && 
						(double)gateWorkerIdList.size()/(double)procWorkerIdList.size()<=GATEWAY_WORKER_RATIO_MIN)){
				newWM.setGateway();
				gateWorkerIdList.add(newWM);
				Link link=workerLinkMap.get(newWM.getId());
				GatewayRoleMessage grMsg=new GatewayRoleMessage(id, newWM.isGateway());
				link.send(grMsg);
			}
			else
				procWorkerIdList.add(newWM);			
		}
	}
	
	public void registerRawStream(RawStream rsl){
		this.rawStreamList.add(rsl);
		if(!rawSamplingWorkerMap.containsKey(rsl.getEventName()) &&
				this.procWorkerIdList.size()>0){
			WorkerId wm=rawSamplingStrategy.nextWorkerId();
			if(wm!=null){
				submitNewRawStreamSamplingMessageToWorker(rsl, wm);
				rawSamplingWorkerMap.put(rsl.getEventName(), wm.getId());
			}
		}
	}
	
	public void submit(StreamContainerFlow scf){
		containerTreeMap.put(scf.getEplId(), scf);
		costEval.registContainerRecursively(scf.getRootContainer());
		delayedContainerFlowRegistry.delayNewStreamContainerFlow(scf);
		submitRecursively(scf.getRootContainer(), scf.getEplId());
	}
	
	public void submitRecursively(StreamContainer sc, long eqlId){
		if(sc instanceof RootStreamContainer){
			RootStreamContainer rsc=(RootStreamContainer)sc;
			costEval.workerInputContainersMap.putPair(rsc.getWorkerId().getId(), (DerivedStreamContainer)rsc.getUpContainer());
			submitRecursively(rsc.getUpContainer(), eqlId);
		}
		else if(sc instanceof JoinDelayedStreamContainer){
			JoinDelayedStreamContainer jcsc=(JoinDelayedStreamContainer)sc;
			costEval.workerInputContainersMap.putPair(jcsc.getWorkerId().getId(), (DerivedStreamContainer)jcsc.getAgent());
			submitRecursively(jcsc.getAgent(), eqlId);
		}
		else if(sc instanceof JoinStreamContainer){
			JoinStreamContainer jsc=(JoinStreamContainer)sc;
			for(StreamContainer csc: jsc.getUpContainerList()){
				costEval.workerInputContainersMap.putPair(jsc.getWorkerId().getId(), (DerivedStreamContainer)csc);
				submitRecursively(csc, eqlId);
			}
		}
		else if(sc instanceof FilterDelayedStreamContainer){
			FilterDelayedStreamContainer fcsc=(FilterDelayedStreamContainer)sc;
			costEval.workerInputContainersMap.putPair(fcsc.getWorkerId().getId(), (DerivedStreamContainer)fcsc.getAgent());
			submitRecursively(fcsc.getAgent(), eqlId);
		}
		else if(sc instanceof FilterStreamContainer){			
			FilterStreamContainer fsc=(FilterStreamContainer)sc;			
			if(!rawSamplingWorkerMap.containsKey(fsc.getRawStream().getEventName())){
				WorkerId wm=fsc.getWorkerId();
				submitNewRawStreamSamplingMessageToWorker(fsc.getRawStream(), wm);
				rawSamplingWorkerMap.put(fsc.getRawStream().getEventName(), wm.getId());
			}
			try{
				costEval.workerInputRawStreamsMap.putPair(fsc.getWorkerId().getId(), fsc.getRawStream());
			}
			catch(Exception ex){
				log.getLogger().error(String.format("fsc.getWorkerId()=%s, fsc.getRawStream()=%s", fsc.getWorkerId(), fsc.getRawStream()), ex);
			}
		}
		StreamContainer sc2=StreamContainerFactory.copy(sc, 2);//FIXME
		submitStreamContainerToWorker(sc2, eqlId);
		//addToExistedStreamContainer(sc);
	}
	
	public Link getWorkerLink(String workerId){
		Link link=workerLinkMap.get(workerId);
		try{
			if(link==null){
				throw new NullPointerException();
			}
		}
		catch(Exception ex){
			log.getLogger().error("Link is null for worker "+workerId, ex);
		}
		return link;
	}
	
	public void submitNewRawStreamSamplingMessageToWorker(RawStream rsl, WorkerId targetWorkerId){
		NewRawStreamSamplingMessage nrssMsg=new NewRawStreamSamplingMessage(id, rsl);
		Link link=getWorkerLink(targetWorkerId.getId());
		link.send(nrssMsg);
	}
	
	public void submitStreamContainerToWorker(StreamContainer sc, long eqlId){	
		Link link=getWorkerLink(sc.getWorkerId().getId());
		DerivedStreamContainer psc=(DerivedStreamContainer)sc;
		
		NewOrModifyInstanceResponseMessage nmirm=new NewOrModifyInstanceResponseMessage(sc.getWorkerId().getId(), sc.getUniqueName(), eqlId, true);
		if(psc.isNew()){
			NewStreamInstanceMessage nsiMsg=new NewStreamInstanceMessage(id,sc,eqlId);
			link.send(nsiMsg);		
		}
		else{
			ModifyStreamInstanceMessage msiMsg=new ModifyStreamInstanceMessage(id,sc,eqlId);
			link.send(msiMsg);
		}
		log.debug(id+" waiting for "+nmirm.toString());
		responseMessageHandler.waitResponse(nmirm);
		log.debug(id+" waited for "+nmirm.toString());
	}
	
	public void addToExistedStreamContainer(StreamContainer sc){
		if(sc instanceof FilterStreamContainer && !this.existedFscList.contains(sc)){
			this.existedFscList.add((FilterStreamContainer)sc);
		}
		else if(sc instanceof PatternStreamContainer && !this.existedPscList.contains(sc)){
			this.existedPscList.add((PatternStreamContainer)sc);
		}
		else if(sc instanceof JoinStreamContainer && !this.existedJscList.contains(sc)){
			this.existedJscList.add((JoinStreamContainer)sc);
		}
		else if(sc instanceof RootStreamContainer && !this.existedRscList.contains(sc)){
			this.existedRscList.add((RootStreamContainer)sc);
		}
	}
	
	public void lockContainerMap(){
		this.containerMapLock.lock();
	}
	
	public void unlockContainerMap(){
		this.containerMapLock.unlock();
	}
	
	@Override
	public String toString(){
		return this.getClass().getSimpleName()+"["+id+"]";
	}
	
	public class WorkerAssignmentStrategy{
		int index=0;
		public WorkerId nextWorkerId(){
			if(procWorkerIdList.size()==0){
				return null;
			}
			WorkerId wm=procWorkerIdList.get(index % procWorkerIdList.size());
			index++;
			return wm;
		}
		public WorkerId assginWorker(Stream sl){
			sl.setWorkerId(procWorkerIdList.get(index % procWorkerIdList.size()));
			index++;
			return sl.getWorkerId();
		}
	}
}
