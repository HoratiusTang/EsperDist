package dist.esper.core.message;

public class GatewayRoleMessage extends AbstractMessage {
	private static final long serialVersionUID = -6886453872716180504L;
	boolean gateway;
	
	public GatewayRoleMessage() {
		super();
	}

	public GatewayRoleMessage(String sourceId, boolean isGateway) {
		super(sourceId);
		this.gateway = isGateway;
	}

	public boolean isGateway() {
		return gateway;
	}
	
	public boolean getGateway() {
		return gateway;
	}

	public void setGateway(boolean isGateway) {
		this.gateway = isGateway;
	}
}
