package dist.esper.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.epl.spec.StatementSpecCompiled;

import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.flow.centralized.Tree;
import dist.esper.core.flow.centralized.TreeBuilder;
import dist.esper.core.flow.stream.JoinStream;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.RootStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.core.flow.stream.StreamFlow;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.Worker;
import dist.esper.epl.expr.EventRegistry;
import dist.esper.epl.expr.StatementSpecification;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.event.Event;
import dist.esper.proxy.EPAdministratorImplProxy;

public class TestMappedEventRegistry {
//	public static void main(String[] args){
//		Configuration config = new Configuration();
//		
//		registMappedEvents();
//		Collection<Event> eventSet=ServiceManager.getDefaultInstance().getEventRegistry().getRegistedEvents();
//		for(Event event: eventSet){
//			config.addEventType(event.getName(), event);
//		}
//		
//		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);
//		EPAdministratorImplProxy epa=new EPAdministratorImplProxy(epService.getEPAdministrator());
//		
//		run1(epa);
//	}
//	
//	static Worker worker=null;
//	public static void run1(EPAdministratorImplProxy epa){
//		try{
//			String epl=null;
//			
////			epl = "select a[1].clientIds, a[0].clientIds[1], a[0] , a[1].id, b2.clientIds, b, b.id, c2.id, sum(b.clientIds[0]) " +
////					"from pattern [every([2](a=A) -> b=B -> c=C)], B(id>3).win:length(5) as b2, C(price>14.0).win:time(6) as c2 " +
////					"where a[0].id=b.id and b2.id=b.id and b2.id>c2.id and b2.name=c2.name";
//			
//			epl = "select b2.clientIds, b2.name, b2.id from B(id>3).win:length(5) as b2 where b2.id<5";
//			epl = "select a[0].id, b.clientIds, c from pattern [every([2](a=A) -> b=B -> c=C)]";
//			
//			StatementSpecCompiled compiled=epa.compileQuery(epl);
//			StatementSpecification ss=StatementSpecification.Factory.make(compiled);
//			StatementSementicWrapper ssw=new StatementSementicWrapper(0, ServiceManager.getDefaultInstance().getEventRegistry());
//			ss.resolve(ssw, null);
//			
//			System.out.println(ss.toString());
//			
//			TreeBuilder builder=new TreeBuilder(ss.getEplId(), epl, ss);
//			builder.buildTreeList();
//			List<Tree> treeList=builder.getTreeList();
//			
//			for(Tree tree: treeList){
//				System.out.println(tree.toString());
//			}
//			
//			Coordinator coord=genCoordinator();
//			List<StreamTree> stList=coord.buildStreamTree(treeList.get(0));
//			
//			for(StreamTree st: stList){
//				System.out.println(st.toString());
//			}
//			
//			worker=genWorker();
//			recursivelyHandleStreamLocaction(stList.get(0).getRoot());
//		}
//		catch(Exception ex){
//			ex.printStackTrace();
//		}
//	}
//	
//	public static void recursivelyHandleStreamLocaction(StreamLocation sl){
////		worker.handleNewStreamLocation(sl);
////		if(sl instanceof RootStreamLocation){
////			RootStreamLocation rsl=(RootStreamLocation)sl;
////			worker.handleNewStreamLocation(rsl.getChild());
////		}
////		else if(sl instanceof JoinStreamLocation){
////			JoinStreamLocation jsl=(JoinStreamLocation)sl;
////			for(StreamLocation csl: jsl.getChildList()){
////				worker.handleNewStreamLocation(csl);
////			}
////		}
//	}
//	
//	public static Coordinator genCoordinator(){
//		Coordinator coord=new Coordinator(ServiceManager.getCoordinatorId());
//		List<WorkerMeta> workerList=new ArrayList<WorkerMeta>(4);
//		ServiceManager.getDefaultInstance().registerWorkerMeta(new WorkerMeta("worker1","127.0.0.1",1001));
//		ServiceManager.getDefaultInstance().registerWorkerMeta(new WorkerMeta("worker2","127.0.0.1",1002));
//		ServiceManager.getDefaultInstance().registerWorkerMeta(new WorkerMeta("worker3","127.0.0.1",1003));
//		
//		List<Event> eventList=new ArrayList<Event>(ServiceManager.getDefaultInstance().getEventRegistry().getRegistedEvents());
//		
//		List<RawStreamLocation> rslList=new ArrayList<RawStreamLocation>(4);
//		rslList.add(new RawStreamLocation(new WorkerMeta("source1","127.0.0.1",2001), eventList.get(0)));
//		rslList.add(new RawStreamLocation(new WorkerMeta("source2","127.0.0.1",2002), eventList.get(1)));
//		rslList.add(new RawStreamLocation(new WorkerMeta("source3","127.0.0.1",2003), eventList.get(2)));
//		
//		//coord.setWorkerList(workerList);
//		coord.setRawStreamLocationList(rslList);
//		return coord;
//	}
//	
//	public static Worker genWorker(){
//		return new Worker("worker1");
//	}
//	
//	public static void registMappedEvents(){
//		Event a=new Event("A");
//		a.addProperty("id", Integer.class);
//		a.addProperty("name", String.class);
//		a.addProperty("price", Double.class);
//		a.addProperty("time", Long.class);
//		a.addProperty("clientIds", Integer[].class);
//		
//		Event b=new Event("B");
//		b.addProperty("id", Integer.class);
//		b.addProperty("name", Integer.class);
//		b.addProperty("price", Double.class);
//		b.addProperty("time", Long.class);
//		b.addProperty("clientIds", Integer[].class);
//		
//		Event c=new Event("C");
//		c.addProperty("id", Integer.class);
//		c.addProperty("name", Integer.class);
//		c.addProperty("price", Double.class);
//		c.addProperty("time", Long.class);
//		c.addProperty("clientIds", Integer[].class);
//		
//		EventRegistry rg=ServiceManager.getDefaultInstance().getEventRegistry();
//		rg.registEvent(a);
//		rg.registEvent(b);
//		rg.registEvent(c);
//	}
}
