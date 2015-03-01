package dist.esper.core.comm.simulation;

import java.util.*;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.id.WorkerId;
import dist.esper.core.worker.Worker;
import dist.esper.util.Tuple2D;

public class SimutationLinkPool{
	Map<WorkerId,LinkManager> linkManagerMap=new HashMap<WorkerId,LinkManager>();
	List<LinkPair> linkPairList=new ArrayList<LinkPair>();	
	
	public static SimutationLinkPool pool=null;
	public static SimutationLinkPool getInstance(){
		if(pool==null){
			pool=new SimutationLinkPool();
		}
		return pool;
	}
	
	private SimutationLinkPool(){}

	public LinkManager newLinkManager(WorkerId myId){
		SimulationLinkManager lm=new SimulationLinkManager(myId);
		linkManagerMap.put(myId, lm);
		return lm;
	}
	
	public Link newLink(WorkerId senderId, WorkerId receiverId){		
		for(LinkPair pair: linkPairList){
			LinkPair result=pair.match(senderId, receiverId);
			if(result!=null){
				return result.getFirst();
			}
		}
		SimulationLink senderLink=new SimulationLink(senderId, receiverId);
		SimulationLink receiverLink=new SimulationLink(receiverId, senderId);
		senderLink.setPairLink(receiverLink);
		receiverLink.setPairLink(senderLink);
		linkPairList.add(new LinkPair(senderLink, receiverLink));
		
		/*notify receiver LinkManager*/
		LinkManager receiverLinkMng=linkManagerMap.get(receiverId);		
		receiverLinkMng.notifyNewReceivedLink(receiverLink);
		return senderLink;		
	}
	
	class SimulationLinkManager extends LinkManager{
		protected SimulationLinkManager(WorkerId myId) {
			super(myId);
		}

		@Override
		public Link connect(WorkerId targetId) {
			if(!sendLinkMap.containsKey(targetId)){
				Link link=newLink(this.myId, targetId);
				sendLinkMap.put(targetId, link);
				//this.addNewLink(link);//WRONG,don't notify source worker
			}
			return sendLinkMap.get(targetId);			
		}

		@Override
		public Link reconnect(Link oldLink) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void init() {
			// TODO Auto-generated method stub
			
		}
		
	}
	class SimulationLink extends Link{
		SimulationLink pairLink;
		public SimulationLink(WorkerId myId, WorkerId targetId) {
			super(myId, targetId);
		}

		public SimulationLink getPairLink() {
			return pairLink;
		}
		
		public void setPairLink(SimulationLink pairLink) {
			this.pairLink = pairLink;
		}
		
		@Override
		public int send(Object obj) throws RuntimeException {
			pairLink.notifyReceived(obj);
			return LOCAL_TRANSMISSION;
		}

		@Override
		public boolean isConnected() {
			return true;
		}
	}
	
	class LinkPair extends Tuple2D<Link,Link>{
		private static final long serialVersionUID = -6646224638856796672L;
		public LinkPair(Link first, Link second) {
			super(first, second);
			assert(first.getMyId()==second.getTargetId());
			assert(first.getTargetId()==second.getMyId());
		}
		public LinkPair match(WorkerId senderId, WorkerId receiverId){
			if(first.getMyId().equals(senderId) && 
				second.getMyId().equals(receiverId)){
				return this;
			}
			return null;
		}
	}
}
