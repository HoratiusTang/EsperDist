package dist.esper.monitor;

import java.util.concurrent.Semaphore;

import dist.esper.core.comm.Link;
import dist.esper.core.comm.LinkManager;
import dist.esper.core.id.WorkerId;
import dist.esper.core.message.NewMonitorMessage;
import dist.esper.core.message.SubmitEplRequest;
import dist.esper.core.message.SubmitEplResponse;
import dist.esper.core.util.ServiceManager;
import dist.esper.io.GlobalStat;
import dist.esper.monitor.ui.MonitorWindow;
import dist.esper.monitor.ui.SubmitEplHook;

public class MonitorMain {
	static String DEFAULT_ID="monitor";
	static int DEFAULT_PORT=20000;
	
	String id;
	Link coordLink=null;
	LinkManager linkManager;
	CoordinatorLinkHandler coordLinkHandler=new CoordinatorLinkHandler();
	SubmitEplHandler submitEplHandler=new SubmitEplHandler();
	UpdateUIRunnable upUIRun=new UpdateUIRunnable();
	MonitorWindow window;
	
	public static void main(String[] args){
		try {
			ServiceManager.initConfig(args);
			ServiceManager.setSimulation(false);
			String id=regist();
			MonitorMain monitor=new MonitorMain(id);
			monitor.init();
			monitor.start();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			System.out.println("Monitor terminated");
		}
	}
	
	private static String regist() throws Exception{
		String ip=ServiceManager.getLocalhostIP();
		WorkerId wm=new WorkerId(DEFAULT_ID, ip, DEFAULT_PORT);
		ServiceManager.getInstance(DEFAULT_ID).registerWorkerId(wm);
		return DEFAULT_ID;
	}
	
	class CoordinatorLinkHandler implements Link.Listener{
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {}

		@Override
		public void received(Link link, Object obj) {
			if(obj instanceof GlobalStat){
				//System.err.format("%s received GlobalStat from %s\n", id, link.getTargetMeta().toString());
				upUIRun.setGlobalStat((GlobalStat)obj);
				if(!window.getDisplay().isDisposed()){
					window.getDisplay().syncExec(upUIRun);
				}
			}
		}
	}
	
	class SubmitEplResponseHandler implements Link.Listener{
		@Override public void connected(Link link) {}
		@Override public void disconnected(Link link) {}

		Semaphore sem;
		SubmitEplResponse serp;
		SubmitEplRequest serq;
		
		public SubmitEplResponse syncSubmit(SubmitEplRequest serq) throws InterruptedException{
			this.serq=serq;
			serp=null;
			sem=new Semaphore(0);			
			coordLink.send(serq);
			sem.acquire();
			return serp;
		}
		@Override
		public void received(Link link, Object obj) {
			if(obj instanceof SubmitEplResponse){
				serp=(SubmitEplResponse)obj;
				if(serp.getTag()==serq.getTag()){
					sem.release();
				}
			}
		}
	}
	
	class SubmitEplHandler implements SubmitEplHook{
		@Override
		public SubmitEplResponse submit(String epl) throws Exception {
			return submitEpl(epl);
		}
	}
	
	public MonitorMain(String id){
		super();
		this.id = id;
	}
	
	public void init(){
		linkManager=ServiceManager.getInstance(id).getLinkManager();
		linkManager.init();
		
		coordLink=linkManager.connect(ServiceManager.getCoordinatorWorkerId());
		coordLink.addListener(coordLinkHandler);		
		coordLink.send(new NewMonitorMessage());
		
		window=new MonitorWindow();
		window.init();
		window.setSubmitEplHook(submitEplHandler);
	}
	
	public void start(){
		window.display();
	}
	
	public SubmitEplResponse submitEpl(String epl) throws Exception{
		long tag=(long)(Math.random()*Long.MAX_VALUE);
		SubmitEplRequest serq=new SubmitEplRequest(id, epl, tag);
		SubmitEplResponseHandler serqHandler=new SubmitEplResponseHandler();
		coordLink.addListener(serqHandler, SubmitEplResponse.class.getSimpleName());
		SubmitEplResponse serp=serqHandler.syncSubmit(serq);
		coordLink.removeListener(serqHandler, SubmitEplResponse.class.getSimpleName());
		return serp;
	}
	
	class UpdateUIRunnable implements Runnable{
		GlobalStat gs;
		
		public void setGlobalStat(GlobalStat gs) {
			this.gs = gs;
		}

		@Override
		public void run() {
			window.update(gs);
		}
	}
}
