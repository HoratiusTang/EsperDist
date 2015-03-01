package dist.esper.core.worker.pubsub;

import com.espertech.esper.client.EventBean;

public interface IProcessorObserver {
	public void updateProcessorObserver(EventBean[] newEvents);
}
