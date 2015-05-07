package dist.esper.core.comm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentSkipListMap;

import dist.esper.core.id.WorkerId;
import dist.esper.util.Logger2;

public abstract class Link {
	static Logger2 log=Logger2.getLogger(Link.class);
	public static final int LOCAL_TRANSMISSION=0;
	protected long linkId=0;
	protected WorkerId myId=null;
	protected WorkerId targetId=null;
	protected LinkManager linkManager=null;
	protected List<Listener> overallListenerList=new ArrayList<Listener>(4);
	protected Map<String,List<Listener>> listenerListMap=new ConcurrentSkipListMap<String,List<Listener>>();
	private static AtomicLong linkCounter=new AtomicLong(0L);
	
	public Link(){
		this(null, null);
	}
	
	public Link(WorkerId myId, WorkerId targetId) {
		super();
		this.myId = myId;
		this.targetId = targetId;
		this.linkId = linkCounter.getAndIncrement();
	}

	public WorkerId getMyId() {
		return myId;
	}

	public void setMyId(WorkerId myId) {
		this.myId = myId;
	}

	public WorkerId getTargetId() {
		return targetId;
	}

	public void setTargetID(WorkerId targetId) {
		this.targetId = targetId;
	}

	public LinkManager getLinkManager() {
		return linkManager;
	}

	public void setLinkManager(LinkManager linkManager) {
		this.linkManager = linkManager;
	}

	public void addListener(Listener listener){
		synchronized(overallListenerList){
			if(!overallListenerList.contains(listener)){
				overallListenerList.add(listener);
			}
		}
		//remove duplicate listener from map, avoid notify multiple times
		for(Map.Entry<String,List<Listener>> e: listenerListMap.entrySet()){
			List<Listener> lnList=e.getValue();
			synchronized(lnList){
				if(lnList.contains(listener)){
					lnList.remove(listener);
				}
			}
		}
	}
	
	public void removeListener(Listener listener){
		synchronized(overallListenerList){
			overallListenerList.remove(listener);
		}
	}
	
	public List<Listener> getOverallListenerList() {
		return overallListenerList;
	}
	
	public void addListener(Listener listener, String classTypeName){
		List<Listener> lnList=listenerListMap.get(classTypeName);
		if(lnList==null){
			lnList=new ArrayList<Listener>(4);
			listenerListMap.put(classTypeName, lnList);
		}
		synchronized(lnList){
			if(!lnList.contains(listener)){
				lnList.add(listener);
			}
		}
	}
	
	public void removeListener(Listener listener, String classTypeName){		
		List<Listener> lnList=listenerListMap.get(classTypeName);
		synchronized(lnList){
			if(lnList!=null){
				lnList.remove(listener);
			}
		}
	}
	
	public long getLinkId() {
		return linkId;
	}

	public void setLinkId(long linkId) {
		this.linkId = linkId;
	}

	@Override
	public String toString(){
		return String.format("%s(%d):[%s-%s]", 
				getClass().getSimpleName(), linkId, myId, targetId);
	}

	public abstract boolean isClosed();
	public abstract int send(Object obj) throws RuntimeException;
	
	public void notifyReceived(Object obj){
		if(obj.getClass().getSimpleName().equals("NewWorkerMessage") && getOverallListenerList().size()==0){
			log.warn("recevied NewWorkerMessage from %s, but OverallListenerList is empty", targetId.getId());
		}
		synchronized(overallListenerList){
			for(Link.Listener ln: overallListenerList){
				try{
					ln.received(this, obj);
				}
				catch(Exception ex){
					log.getLogger().error(String.format("error occur when notifyReceived(), link: %s", this.toString()), ex);
				}
			}
		}
		List<Listener> lnList=listenerListMap.get(obj.getClass().getSimpleName());
		if(lnList!=null){
			for(Link.Listener ln: lnList){
				try{
					ln.received(this, obj);
				}
				catch(Exception ex){
					log.getLogger().error(String.format("error occur when notifyReceived(), link: %s", this.toString()), ex);
				}
			}
		}
	}
	
	public void notifyDisconnnected(){
		for(Link.Listener ln: getOverallListenerList()){
			ln.disconnected(this);
		}
	}
	
	
	public interface Listener{
		public void connected(Link link);
		public void disconnected(Link link);
		public void received(Link link, Object obj);
	}
}
