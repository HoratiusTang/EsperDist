package dist.esper.external;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.message.DataMessage;
import dist.esper.core.message.NewSpoutMessage;
import dist.esper.core.message.NewWorkerMessage;
import dist.esper.core.message.StartSubscribeMessage;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.pubsub.ISubscriberObserver;
import dist.esper.core.worker.pubsub.Publisher;
import dist.esper.core.worker.pubsub.Subscriber;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;
import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.util.Logger2;

public class Spout {
	static Logger2 log=Logger2.getLogger(Spout.class);
	String id;
	Link coordLink=null;
	LinkManager linkManager;
	Event event;
	EventInstanceGenerator eventInsGenerator;
	Map<String,Link> workerLinkMap=new ConcurrentHashMap<String,Link>(4);
	//List<Link> subLinkList=new ArrayList<Link>();
	Map<String,Publisher> pubMap=new ConcurrentHashMap<String,Publisher>();
	NewLinkHandler newLinkHandler=new NewLinkHandler();
	LinkHandler linkHandler=new LinkHandler();
	int batchCount=20;
	long sendIntervalMS=1000;
	
	class NewLinkHandler implements LinkManager.NewLinkListener{		
		@Override
		public void newReceivedLink(Link link) {
			ServiceManager.getInstance(id).registerWorkerId(link.getTargetId());			
			workerLinkMap.put(link.getTargetId().getId(), link);
			link.addListener(linkHandler, StartSubscribeMessage.class.getSimpleName());
			log.info("Spout %s accept link(%d) from %s", id, link.getLinkId(), link.getTargetId().getId());
		}
	}
	
	class LinkHandler implements Link.Listener{
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {}

		@Override
		public void received(Link link, Object obj) {
			if(obj instanceof StartSubscribeMessage){
				StartSubscribeMessage subMsg=(StartSubscribeMessage)obj;
				Publisher pub=pubMap.get(subMsg.getSourceId());
				if(pub==null){
					pub=new Publisher(id, link, subMsg.getStreamName(), 
							//subMsg.getStreamEventTypeName(),
							subMsg.getSelectElementNameList(),
							null);
					pubMap.put(subMsg.getSourceId(), pub);
				}
				pub.setLink(link);
				pub.setStreamName(subMsg.getStreamName());
				pub.setSelectElementNames(subMsg.getSelectElementNameList());
			}
		}
	}	
	
	class PublishRunner implements Runnable{
		@Override
		public void run() {
			log.info("Spout %s is running, sending %s", id, event.getName());
			while(true){
				try {
					Object[] objs=new Object[batchCount];
					for(int i=0;i<batchCount;i++){
						TreeMap<String,Object> obj=eventInsGenerator.nextEventInstance();
						objs[i]=obj;
					}
					//DataMessage dataMsg=new DataMessage(id, event.getName(), objs);
					publish(objs);
					Thread.sleep(sendIntervalMS);
				}
				catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void publish(Object[] objs){
		for(Publisher pub: pubMap.values()){
			DataMessage dataMsg=new DataMessage(id, 
					pub.getStreamName(), 
					new String[]{this.event.getName()},
					objs);
			pub.getLink().send(dataMsg);//FIXME
		}
	}
	
	public Spout(String id, EventInstanceGenerator eventInsGenerator){
		this.id=id;
		this.event=eventInsGenerator.getEvent();
		this.eventInsGenerator=eventInsGenerator;
	}
	
	public void init(){
		linkManager=ServiceManager.getInstance(id).getLinkManager();
		linkManager.init();
		linkManager.setNewLinkListener(newLinkHandler);
		
		coordLink=linkManager.connect(ServiceManager.getCoordinatorWorkerId());
		//coordLink.addListener(coordLinkHandler);
		coordLink.send(new NewSpoutMessage(id, event));
	}
	
	public void start(){
		start(true);
	}
	
	public void start(boolean sync){
		if(sync){
			new PublishRunner().run();
		}
		else{
			new Thread(new PublishRunner()).start();
		}
		log.info("Spout %s is running...", this.id);
	}
	
	@Override
	public String toString(){
		//return this.getClass().getSimpleName()+"["+id+","+"]";
		return String.format("%s[%s,%s]", this.getClass().getSimpleName(), id, event.getName());
	}
}
