package dist.esper.core.comm;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import dist.esper.core.id.WorkerId;

public abstract class LinkManager {
	protected WorkerId myId;	
	protected NewLinkListener newLinkListener=new DefaultNewLinkListener();
	protected Map<WorkerId,Link> sendLinkMap=new ConcurrentHashMap<WorkerId,Link>();//indexed by targetId, store the link, not for send
	protected Map<WorkerId,Link> recvLinkMap=new ConcurrentHashMap<WorkerId,Link>();//indexed by targetId, store the link, not for send
	
	public LinkManager(WorkerId myId){
		this.myId = myId;
	}
	
	public NewLinkListener getNewLinkListener() {
		return newLinkListener;
	}

	public void setNewLinkListener(NewLinkListener newLinkListener) {
		this.newLinkListener = newLinkListener;
	}
	
	@Override
	public String toString(){
		return String.format("%s:[%s: sendLinkMap=%s, recvLinkMap=%s]",
				getClass().getSimpleName(), myId, sendLinkMap.toString(), recvLinkMap.toString());
	}

	public abstract void init();
	public abstract Link connect(WorkerId targetId);
	public abstract Link reconnect(Link oldLink);
	protected abstract Link newLink(WorkerId myId, WorkerId targetId, Object...  args);
	
	public void notifyNewReceivedLink(Link link){
		recvLinkMap.put(link.getTargetId(), link);
		if(newLinkListener!=null){
			newLinkListener.newReceivedLink(link);
		}
	}
	
	public interface NewLinkListener{
		public void newReceivedLink(Link link);
	}
	
	class DefaultNewLinkListener implements NewLinkListener{
		@Override public void newReceivedLink(Link link) {}
	}	
}
