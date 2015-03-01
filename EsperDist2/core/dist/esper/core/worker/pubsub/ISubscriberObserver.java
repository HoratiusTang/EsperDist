package dist.esper.core.worker.pubsub;

public interface ISubscriberObserver {
	public void updateSubscriberObserver(long subscriberId, String streamName, String[] elementNames, String internalEventTypeName, Object[] events);//Object[][] or Object[]
}
