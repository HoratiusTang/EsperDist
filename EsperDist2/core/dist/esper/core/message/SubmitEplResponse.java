package dist.esper.core.message;

public class SubmitEplResponse extends AbstractMessage {
	private static final long serialVersionUID = -116415656328695829L;

	long tag;
	long eplId;//eplId<0 indicates failed
	String info;
	
	public SubmitEplResponse() {
		super();
	}

	public SubmitEplResponse(String sourceId, long tag, long eplId, String info) {
		super(sourceId);
		this.tag = tag;
		this.eplId = eplId;
		this.info = info;
	}

	public long getTag() {
		return tag;
	}

	public void setTag(long tag) {
		this.tag = tag;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}
	
	@Override
	public String toString(){
		return String.format("%s[sourceId=%s, tag=%d, eplId=%d, info=%s]", 
				this.getClass().getSimpleName(), sourceId, tag, eplId, info);
	}
}
