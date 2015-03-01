package dist.esper.core.message;

import dist.esper.core.id.WorkerId;

public class RegisterWorkerMessage extends AbstractMessage{
	private static final long serialVersionUID = 7821598727697653639L;

	public RegisterWorkerMessage() {
		super();
	}

	public RegisterWorkerMessage(String sourceId) {
		super(sourceId);
	}

	WorkerId workerId;

	public WorkerId getWorkerId() {
		return workerId;
	}

	public void setWorkerId(WorkerId workerId) {
		this.workerId = workerId;
	}
}
