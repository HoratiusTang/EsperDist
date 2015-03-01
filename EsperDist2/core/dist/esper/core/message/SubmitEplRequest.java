package dist.esper.core.message;

public class SubmitEplRequest extends AbstractMessage {
	private static final long serialVersionUID = -116415656328695829L;
	long tag;
	String epl;
	public SubmitEplRequest() {
		super();
	}

	public SubmitEplRequest(String sourceId, String epl, long tag) {
		super(sourceId);
		this.epl = epl;
		this.tag = tag;
	}

	public String getEpl() {
		return epl;
	}

	public void setEpl(String epl) {
		this.epl = epl;
	}

	public long getTag() {
		return tag;
	}

	public void setTag(long tag) {
		this.tag = tag;
	}
}
