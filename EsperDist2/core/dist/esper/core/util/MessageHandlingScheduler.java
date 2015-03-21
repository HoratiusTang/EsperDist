package dist.esper.core.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import dist.esper.core.comm.Link;
import dist.esper.core.worker.pubsub.ProcessingScheduler2;
import dist.esper.util.Logger2;

public class MessageHandlingScheduler {
	static Logger2 log=Logger2.getLogger(ProcessingScheduler2.class);
	String workerId;
	boolean isSync=false;
	ThreadPoolExecutor pool;
	public int numThreads=1;
	
	public MessageHandlingScheduler(String workerId){
		this(workerId, 1);
	}
	public MessageHandlingScheduler(String workerId, int numThreads){
		this.workerId = workerId;
		this.numThreads = numThreads;
		pool=(ThreadPoolExecutor)Executors.newFixedThreadPool(numThreads);
		log.info("Worker %s start ProcessingScheduler2 with numThreads=%d", workerId, numThreads);
	}
	
	public Runnable newMessageHandlingTask(Link link, Object obj, IMessageHandler mh){
		MessageHandlingTask mht=new MessageHandlingTask(link, obj, mh);
		return mht;
	}
	
	public void submit(Link link, Object obj, IMessageHandler mh){
		Runnable task=newMessageHandlingTask(link, obj, mh);
		if(isSync){
			task.run();
		}
		else{
			pool.submit(task);
		}
	}
	
	public int getQueueSize(){
		return pool.getQueue().size();
	}
	
	public int getNumberThreads(){
		return numThreads;
	}
	
	class MessageHandlingTask implements Runnable{
		Link link;
		Object obj;
		IMessageHandler mh;
		
		public MessageHandlingTask(Link link, Object obj, IMessageHandler mh) {
			super();
			this.link = link;
			this.obj = obj;
			this.mh = mh;
		}

		@Override
		public void run() {
			mh.handleMessage(link, obj);
		}
	}
	
	public static interface IMessageHandler{
		public void handleMessage(Link link, Object obj);
	}
}
