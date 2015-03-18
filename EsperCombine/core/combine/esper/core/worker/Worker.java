package combine.esper.core.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

import combine.esper.core.message.NewInstanceMessage;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.message.NewWorkerMessage;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.pubsub.ProcessingScheduler2;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;
import dist.esper.util.Logger2;

public class Worker {
	static Logger2 log=Logger2.getLogger(Worker.class);
	public String id;
	public Link coordLink=null;
	public LinkManager linkManager;
	public EPServiceProvider epService;
	//MappedEventRegistry eventRegistry=null;
	//public Map<String,Stream> slMap=new HashMap<String,Stream>();
	public Map<String,Instance> insMap=new HashMap<String,Instance>();
	public CoordinatorLinkHandler coordLinkHandler=new CoordinatorLinkHandler();	
	ProcessingScheduler2 procScheduler;
	WorkerStatCollector workerStatCollector;
	WorkerStatReportor workerStatReportor;

	public static EventOrPropertySpecComparator epsComparator=new EventOrPropertySpecComparator();
	
	class CoordinatorLinkHandler implements Link.Listener{
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {}

		@Override
		public void received(Link link, Object obj) {
			handleCoordinatorMessage(obj);
		}
	}	
	
	public Worker(String id) {
		super();
		this.id = id;
	}	
	
	public void init(){
		epService=EPServiceProviderManager.getProvider(id);
		int numProcThread=(int)ServiceManager.getConfig().getLong(Options.WORKER_NUMBER_OF_PROCESS_THREADS,2);
		procScheduler=new ProcessingScheduler2(id, numProcThread);
		linkManager=ServiceManager.getInstance(id).getLinkManager();
		linkManager.init();		
		
		workerStatCollector=new WorkerStatCollector(this, procScheduler);
		
		coordLink=linkManager.connect(ServiceManager.getCoordinatorWorkerId());
		coordLink.addListener(coordLinkHandler);
		coordLink.send(new NewWorkerMessage());
		
		workerStatReportor=new WorkerStatReportor(this);
	}
	public void start(){
		start(true);
	}
	public void start(boolean sync){
		if(sync){		
			workerStatReportor.run();
		}
		else{
			new Thread(workerStatReportor).start();
		}
		log.info("Worker %s is running...", this.id);
	}
	
	public void registEventRecursively(Event event){
		for(EventProperty prop: event.getPropList()){
			if(!prop.isArray() && (prop.getComponentType() instanceof Event)){
				registEventRecursively((Event)prop.getComponentType());
			}
		}
		for(EventProperty prop: event.getPropList()){
			if(prop.isArray() && (prop.getComponentType() instanceof Event)){
				registEventRecursively((Event)prop.getComponentType());
			}
		}
		registExplicitEvent(event);
	}
	
	public void registExplicitEvent(Event event){
		event.refresh();
		ServiceManager.getInstance(id).getEventRegistry().registEvent(event);
		epService.getEPAdministrator().getConfiguration().addEventType(event.getName(), event);
	}

	public void handleCoordinatorMessage(Object obj){
		if(obj instanceof NewInstanceMessage){
			NewInstanceMessage nim=(NewInstanceMessage)obj;
			log.info("Worker %s received epl %d: %s", id, nim.getEplId(), nim.getEpl());
			startNewInstance(nim.getEplId(), nim.getEpl(), nim.getRawStreamList());
		}
	}
	
	public void startNewInstance(long eplId, String epl, List<RawStream> rawStreamList){
		for(RawStream rs: rawStreamList){
			Event event=rs.getEvent();
			event.refresh();
			this.registExplicitEvent(event);
		}
		
		Instance ins=new Instance(id, linkManager, epService, 
				rawStreamList, 
				this.procScheduler, eplId, epl);
		try {
			ins.init();
			ins.start();
		}
		catch (Exception e) {
			log.getLogger().error(
					String.format("error ocurr when initialize/start instance for epl %d: %s", eplId, epl),	e);
		}
	}
	
	@Override
	public String toString(){
		return this.getClass().getSimpleName()+"["+id+"]";
	}
}
