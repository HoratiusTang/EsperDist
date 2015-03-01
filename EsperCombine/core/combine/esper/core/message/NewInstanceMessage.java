package combine.esper.core.message;

import java.util.List;

import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.message.AbstractMessage;

public class NewInstanceMessage extends AbstractMessage {
	private static final long serialVersionUID = -2856861509519703423L;
	long eplId;
	String epl;
	List<RawStream> rawStreamList;
	
	public NewInstanceMessage() {
		super();
	}

	public NewInstanceMessage(String sourceId) {
		super(sourceId);
	}	

	public NewInstanceMessage(long eplId, String epl, List<RawStream> rawStreamList) {
		super();
		this.eplId = eplId;
		this.epl = epl;
		this.rawStreamList = rawStreamList;
	}

	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}

	public String getEpl() {
		return epl;
	}

	public void setEpl(String epl) {
		this.epl = epl;
	}

	public List<RawStream> getRawStreamList() {
		return rawStreamList;
	}

	public void setRawStreamList(List<RawStream> rawStreamList) {
		this.rawStreamList = rawStreamList;
	}
}
