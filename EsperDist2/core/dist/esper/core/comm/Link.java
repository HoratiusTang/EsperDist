package dist.esper.core.comm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import dist.esper.core.comm.socket.SocketLink;
import dist.esper.core.id.WorkerId;
import dist.esper.util.Logger2;

public abstract class Link {
	static Logger2 log=Logger2.getLogger(Link.class);
	public static final int LOCAL_TRANSMISSION=0;
	protected long linkId=0;
	protected WorkerId myId=null;
	protected WorkerId targetId=null;
	protected List<Listener> overallListenerList=new ArrayList<Listener>(4);
	protected Map<String,List<Listener>> listenerListMap=new TreeMap<String,List<Listener>>();
	private static AtomicLong linkCounter=new AtomicLong(0L);
	
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

	public void addListener(Listener listener){
		if(!overallListenerList.contains(listener)){
			overallListenerList.add(listener);
		}
		//remove duplicate listener from map, avoid notify multiple times
		for(Map.Entry<String,List<Listener>> e: listenerListMap.entrySet()){
			if(e.getValue().contains(listener)){
				e.getValue().remove(listener);
			}
		}
	}
	
	public void removeListener(Listener listener){
		overallListenerList.remove(listener);
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
		if(!lnList.contains(listener)){
			lnList.add(listener);
		}
	}
	
	public void removeListener(Listener listener, String classTypeName){
		List<Listener> lnList=listenerListMap.get(classTypeName);
		if(lnList!=null){
			lnList.remove(listener);
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

	public abstract boolean isConnected();
	public abstract int send(Object obj) throws RuntimeException;
	
	public void notifyReceived(Object obj){
		for(Link.Listener ln: getOverallListenerList()){
			try{
				ln.received(this, obj);
			}
			catch(Exception ex){
				log.getLogger().error(String.format("error occur when notifyReceived(), link: %s", this.toString()), ex);
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
