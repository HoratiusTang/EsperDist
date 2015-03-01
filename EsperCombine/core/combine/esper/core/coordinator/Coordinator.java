package combine.esper.core.coordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

import combine.esper.core.WorkerMain;
import combine.esper.core.message.NewInstanceMessage;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.cost.WorkerStat;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.GatewayRoleMessage;
import dist.esper.core.message.NewMonitorMessage;
import dist.esper.core.message.NewSpoutMessage;
import dist.esper.core.message.NewWorkerMessage;
import dist.esper.core.message.SubmitEplRequest;
import dist.esper.core.message.SubmitEplResponse;
import dist.esper.core.util.ServiceManager;
import dist.esper.event.Event;
import dist.esper.proxy.EPAdministratorImplProxy;
import dist.esper.util.Logger2;

public class Coordinator {
	public static final double GATEWAY_WORKER_RATIO_MIN=1.0d/3.0d;
	static Logger2 log=Logger2.getLogger(Coordinator.class);
	
	public String id;
	LinkManager linkManager;
	public EPServiceProvider epService;
	EPAdministratorImplProxy epAdminProxy;	
	
	Map<String,Link> workerLinkMap=new ConcurrentSkipListMap<String,Link>();
	Map<String,Link> spoutLinkMap=new ConcurrentSkipListMap<String,Link>();
	Map<String,Link> monitorLinkMap=new ConcurrentSkipListMap<String,Link>();
	
	List<RawStream> rawStreamList=new ArrayList<RawStream>();
	AtomicLong selectElementUID=new AtomicLong(0L);
	AtomicLong streamUID=new AtomicLong(0L);
	AtomicLong eplUID=new AtomicLong(0L);
	NewLinkHandler newLinkHandler=new NewLinkHandler();
	List<WorkerId> procWorkerIdList=new ArrayList<WorkerId>(8);
	List<WorkerId> gateWorkerIdList=new ArrayList<WorkerId>(4);	
	
	//Map<String,String> rawSamplingWorkerMap=new HashMap<String,String>();
	WorkerAssignmentStrategy workerAssignStrategy=new WorkerAssignmentStrategy();

	class NewLinkHandler implements LinkManager.NewLinkListener, Link.Listener{
		@Override public void connected(Link link) {}
		@Override 
		public void disconnected(Link link) {
			if(link.getTargetId().getType()==WorkerId.SPOUT)
				spoutLinkMap.remove(link.getTargetId().getId());
			else if(link.getTargetId().getType()==WorkerId.MONITOR)
				monitorLinkMap.remove(link.getTargetId().getId());
			else
				workerLinkMap.remove(link.getTargetId().getId());
		}
		
		@Override
		public void newReceivedLink(Link link) {
			//workerLinkMap.put(link.getTargetId().getId(), link);
			link.addListener(this);
			log.info("%s accept link(%s) from %s", id, link.getLinkId(), link.getTargetId().getId());
		}

		@Override
		public void received(Link link, Object obj) {
			handleReceiving(link, obj);
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
			log.debug("%s received eql %s with tag %d", id, serq.getEpl(), serq.getTag());
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
		}
	}
	
	public Coordinator(String id){
		this.id=id;
	}
	
	public void init(){
		epService = EPServiceProviderManager.getProvider(id);
		epAdminProxy = new EPAdministratorImplProxy(epService.getEPAdministrator());		
		linkManager=ServiceManager.getInstance(id).getLinkManager();
		linkManager.init();
		linkManager.setNewLinkListener(newLinkHandler);		
	}
	
	public void start(){
		start(true);
	}
	public void start(boolean sync){
		if(sync){
			//coordStatReportor.run();
		}
		else{
			//new Thread(coordStatReportor).start();
		}
		log.info("Coordinator is running...");
	}
	
	public void registEPEvent(Event event){
		ServiceManager.getInstance(id).getEventRegistry().registEvent(event);
		epService.getEPAdministrator().getConfiguration().addEventType(event.getName(), event);
	}
	
	public long executeEPL(String epl) throws Exception{
		long eplId=eplUID.getAndIncrement();
		WorkerId wm=workerAssignStrategy.nextWorkerId();
		Link link=workerLinkMap.get(wm.getId());
		NewInstanceMessage nim=new NewInstanceMessage(eplId, epl, this.rawStreamList);
		link.send(nim);
		
		return eplId;
	}	
	
	public List<RawStream> getRawStreamList() {
		return rawStreamList;
	}
	
	public void registerWorkerId(WorkerId newWM){
		ServiceManager.getInstance(id).registerWorkerId(newWM);
		int index=-1;
		for(int i=0;i<procWorkerIdList.size();i++){
			if(newWM.getId().equals(procWorkerIdList.get(i).getId()))
				index=i; break;
		}
		if(index>=0)
			procWorkerIdList.set(index, newWM);
		
		for(int i=0;i<gateWorkerIdList.size();i++){
			if(newWM.getId().equals(gateWorkerIdList.get(i).getId()))
				index=i; break;
		}
		if(index>=0)
			gateWorkerIdList.set(index, newWM);
		
		if(index<0){
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
