package dist.esper.core.id;

import java.io.Serializable;

public class WorkerId implements Serializable {
	private static final long serialVersionUID = -5847597007489605833L;
	
	public static final int SPOUT=1;
	public static final int PROCESSOR=2;
	public static final int GATEWAY=3;
	public static final int MONITOR=4;
	
	public String id="";
	public String ip="";
	public int port=0;
	public int type=PROCESSOR;
	public WorkerId(){
		super();
	}
	public WorkerId(String id, String ip, int port) {
		super();
		this.id = id;
		this.ip = ip;
		this.port = port;
	}
	public WorkerId(String id, String ip, int port, int type) {
		this(id, ip, port);
		this.type = type;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}	
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public boolean isSpout(){
		return type==SPOUT;
	}
	
	public boolean isProcessor(){
		return type==PROCESSOR;
	}
	
	public boolean isGateway() {
		return type==GATEWAY;
	}
	
	public boolean isMonitor() {
		return type==MONITOR;
	}
	
	public void setSpout(){
		type=SPOUT;
	}
	
	public void setProcessor(){
		type=PROCESSOR;
	}
	
	public void setGateway(){
		type=GATEWAY;
	}
	
	public void setMonitor(){
		type=MONITOR;
	}
	
	@Override
	public String toString(){
		return String.format("[%s:%s,%d]", id, ip, port);
	}
	@Override
	public int hashCode(){
		return id.hashCode();
	}
	@Override
	public boolean equals(Object obj){
		if(obj instanceof WorkerId){
			WorkerId wm=(WorkerId)obj;
			return this.id.equals(wm.id);
		}
		return false;
	}
	
}
