package dist.esper.core.worker.pubsub;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import com.espertech.esper.client.EventBean;

import dist.esper.util.Logger2;

public class PublishingScheduler2 {
	static Logger2 log=Logger2.getLogger(PublishingScheduler2.class);
	String workerId;
	boolean isSync=false;
	ThreadPoolExecutor pool;
	public int numThreads=1;
	
	public PublishingScheduler2(String workerId){
		this(workerId, 1);
	}
	public PublishingScheduler2(String workerId, int numThreads){
		this.workerId = workerId;
		this.numThreads = numThreads;
		pool=(ThreadPoolExecutor)Executors.newFixedThreadPool(numThreads);
		log.info("Worker %s start PublishingScheduler2 with numThreads=%d", workerId, numThreads);
	}
	
	public int getQueueSize(){
		return pool.getQueue().size();
	}
	
	public int getNumberThreads(){
		return numThreads;
	}
	
	public Runnable newPublishingRunnable(EventBean[] newEvents, Publisher pub){
		return new PublishingTask(newEvents, pub);
	}
	
	public void sumbit(EventBean[] newEvents, IProcessorObserver pub){
		Runnable task=newPublishingRunnable(newEvents, (Publisher)pub);
		if(isSync){
			task.run();
		}
		else{
			pool.submit(task);
		}
	}
	
	class PublishingTask implements Runnable{
		EventBean[] newEvents;
		Publisher pub;		
		public PublishingTask(EventBean[] newEvents, Publisher pub) {
			super();
			this.newEvents = newEvents;
			this.pub = pub;
		}

		@Override
		public void run() {
			pub.updateProcessorObserver(newEvents);			
		}		
	}
}
