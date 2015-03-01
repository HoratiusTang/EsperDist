package dist.esper.test;

import java.util.*;

import dist.esper.core.CoordinatorMain;
import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.Worker;
import dist.esper.event.Event;
import dist.esper.event.EventRegistry;
import dist.esper.external.Spout;
import dist.esper.io.GlobalStat;
import dist.esper.io.KryoFileWriter;
import dist.esper.util.Tuple2D;

public class TestSimulation {
	List<Event> eventList=new ArrayList<Event>();
	List<Worker> workerList=new ArrayList<Worker>();
	List<Spout> spoutList=new ArrayList<Spout>();
	Coordinator coord=null;
	public static void main(String[] args){
		TestSimulation sim=new TestSimulation();
		sim.init();
		sim.run();
	}
	
	public void init(){
		ServiceManager.coordinatorId=
			new WorkerId(ServiceManager.getCoordinatorId(),"127.0.0.1",1000);
		WorkerId[] workerMetas=new WorkerId[]{
			new WorkerId("worker1","127.0.0.1",1001),
			new WorkerId("worker2","127.0.0.1",1002),
			new WorkerId("worker3","127.0.0.1",1003),
			new WorkerId("worker4","127.0.0.1",1004),
			new WorkerId("worker5","127.0.0.1",1005),
		};
		
		for(WorkerId wm: workerMetas){
			ServiceManager.getInstance(wm.getId()).registerWorkerId(wm);//register itself
		}
		
		coord=new Coordinator(ServiceManager.getCoordinatorId());
		for(WorkerId wm: workerMetas){
			Worker worker=new Worker(wm.getId());
			workerList.add(worker);
		}
		
		coord.init();
		for(Worker w: workerList){
			w.init();
		}
		
		genEvents();
		for(int i=0;i<eventList.size();i++){
			String id="spout"+(i+1);
			Event event=eventList.get(i);
			WorkerId spoutMeta=new WorkerId(id,"127.0.0.1",2001+i);
			spoutMeta.setSpout();
			ServiceManager.getInstance(spoutMeta.getId()).registerWorkerId(spoutMeta);//register itself
			Spout spout=new Spout(id, null);//FIXME
			RawStream rsl=new RawStream(spoutMeta, event);
			spoutList.add(spout);
			spout.init();
			
			coord.registEPEvent(event);
			coord.registerRawStream(rsl);
		}
	}
	
	public void genEvents(){
		Event a=new Event("A");
		a.addProperty("id", Integer.class);
		a.addProperty("name", String.class);
		a.addProperty("price", Double.class);
		a.addProperty("time", Long.class);
		a.addProperty("clientIds", int[].class);
		
		Event b=new Event("B");
		b.addProperty("id", Integer.class);
		b.addProperty("name", String.class);
		b.addProperty("price", Double.class);
		b.addProperty("time", Long.class);
		b.addProperty("clientIds", int[].class);
		
		Event c=new Event("C");
		c.addProperty("id", Integer.class);
		c.addProperty("name", String.class);
		c.addProperty("price", Double.class);
		c.addProperty("time", Long.class);
		c.addProperty("clientIds", int[].class);
		
//		Event d=new Event("D");
//		c.addProperty("id", Integer.class);
//		c.addProperty("name", String.class);
//		c.addProperty("price", Double.class);
//		c.addProperty("time", Long.class);
//		c.addProperty("clientIds", int[].class);
		
		eventList.add(a);
		eventList.add(b);
		eventList.add(c);
//		eventList.add(d);
	}
	
	public void run(){
		for(Spout spout: spoutList){
			spout.start(false);
		}
		//		epl1="select a1.id, a1.name, b1.id, b1.name " +
		//		"from A(id>5).win:length(6) as a1, B(id>3).win:time(4) as b1 where a1.id=b1.id";
		//epl2="select a5[0].id, a5[1].name, b5.id from pattern [every([2]a5=A -> b5=B(id>a5[1].id))].win:time(5)";
		//epl3="select a6[0].id, a6[1].name, b6.id, b6.id, b6.name, b7.id " +
		//		"from pattern [every([2]a6=A -> b7=B(id>a6[1].id))].win:time(5), " +
		//		"B(id>3).win:length(5) as b6 " +
		//		"where b7.id=b6.id+2";
		//epl4="select b2.id, b2.clientIds from B(id>=3).win:length(5) as b2";
		//epl5="select a4.id, a4.clientIds, b4.id, b4.name, b4.clientIds " +
		//		"from A(id>5).win:length(6) as a4, B(id>3).win:time(4) as b4 where a4.id=b4.id";
		//epl6="select b3.id, b3.name from B(id>=3).win:length(5) as b3 where b3.id>5";
		//epl7="select b8.id, b8.name from B(id>=5).win:length(5) as b8";
		//epl9="select b9.id, b9.name from B(id>3 and price>10.0).win:length(5) as b9";
		//epl10="select a10.id, a10.clientIds, b10.id, b10.name, b10.clientIds " +
		//		"from A(id>5).win:length(6) as a10, B(id>3).win:time(4) as b10 where a10.id=b10.id";
		
		String epl00=null, epl01=null, epl02=null, epl03=null, epl04=null, epl05=null, epl06=null, epl07=null, epl08=null, epl09=null;
		String epl10=null, epl11=null, epl12=null, epl13=null, epl14=null, epl15=null, epl16=null, epl17=null, epl18=null, epl19=null;
		String epl20=null, epl21=null, epl22=null, epl23=null, epl24=null, epl25=null, epl26=null, epl27=null, epl28=null, epl29=null;
		epl00="select b00.id, b00.name from B(id>3 or price>6.0).win:time(5 sec) as b00";
		epl01="select b01.id, b01.price from B(id>3).win:time(5 sec) as b01";
		epl02="select b02.id, b02.clientIds from B(id>=3).win:time(5 sec) as b02";
		epl03="select b03.id, b03.clientIds, b03.time from B(id>=3).win:time(5 sec) as b03";
		epl04="select b04.id, b04.time from B(id>=5 and price>5.0).win:time(5 sec) as b04 where b04.time>10";
		epl05="select b05.id, b05.time from B(price>6.0 and id>5).win:time(5 sec) as b05";
		epl06="select b06.id, b06.clientIds, b06.time from B(id>=4).win:time(5 sec) as b06";
		
		epl10="select a00.id, a00.name from A(id>6).win:time(5 sec) as a00";
		epl11="select a01.id, a01.time, a01.price from A(id>5).win:time(5 sec) as a01";
		epl12="select a02.id, a02.price, a02.clientIds from A(id>5 and time>5).win:time(5 sec) as a02";
		
		epl20="select a20.id, a20.clientIds, b20.id, b20.name, b20.clientIds " +
				"from A(id>5).win:time(6 sec) as a20, B(id>3).win:time(5 sec) as b20 where a20.id=b20.id";
		epl21="select a21.id, a21.clientIds, b21.id, b21.name, b21.clientIds " +
				"from A(id>6 or price>10).win:time(6 sec) as a21, B(id>3).win:time(5 sec) as b21 where a21.id=b21.id";
		epl22="select a22.id, a22.clientIds, b22.id, b22.name, b22.price " +
				"from A(id>6).win:time(6 sec) as a22, B(id>7).win:time(5 sec) as b22 where a22.id=b22.id";
		epl23="select a23.id, a23.clientIds, b23.id, b23.name, b23.time " +
				"from A(id>6).win:time(6 sec) as a23, B(id>7).win:time(5 sec) as b23 where a23.id=b23.id";
		epl24="select a24.id, a24.clientIds, b24.id, b24.name, b24.time " +
				"from A(id>6).win:time(6 sec) as a24, B(id>7).win:time(5 sec) as b24 where a24.id=b24.id and a24.name=b24.name";
		
		Pair[] eplPairs=new Pair[]{
				new Pair(00,epl00),
				new Pair(01,epl01),
				new Pair(02,epl02),
				new Pair(03,epl03),
				new Pair(04,epl04),
				new Pair(05,epl05),
				new Pair(06,epl06),
				new Pair(07,epl07),
				new Pair( 8,epl08),
				new Pair( 9,epl09),
				new Pair(10,epl10),
				new Pair(11,epl11),
				new Pair(12,epl12),
				new Pair(13,epl13),
				new Pair(14,epl14),
				new Pair(15,epl15),
				new Pair(16,epl16),
				new Pair(17,epl17),
				new Pair(18,epl18),
				new Pair(19,epl19),
				new Pair(20,epl20),
				new Pair(21,epl21),
				new Pair(22,epl22),
				new Pair(23,epl23),
				new Pair(24,epl24),
				new Pair(25,epl25),
				new Pair(26,epl26),
				new Pair(27,epl27),
				new Pair(28,epl28),
				new Pair(29,epl29),
		};
		
		sleep(6000);
		//randomSort(eplPairs);
		//eplPairs=new Pair[]{};
		//eplPairs=new Pair[]{new Pair(23,epl23), new Pair(22,epl22)};
		//eplPairs=new Pair[]{eplPairs[02], eplPairs[20]};
		//eplPairs=new Pair[]{new Pair(22,epl22)};
		eplPairs=new Pair[]{new Pair(01,epl01), new Pair(20,epl20), new Pair(22,epl22)};
		int eplId=-1;
		try {
			for(int i=0;i<eplPairs.length;i++){
				if(eplPairs[i]!=null && eplPairs[i].getSecond()!=null){
					eplId=eplPairs[i].getFirst();
					System.err.println(eplId);
					printPairs(eplPairs);
					coord.executeEPL(eplPairs[i].getSecond());
					sleep(6000);
				}
			}
			writeGlobalStat(coord);
			System.out.print("");
		}
		catch (Exception e) {
			System.out.println("stopped at "+eplId);
			e.printStackTrace();
			System.exit(0);
		}
	}
	public static void printPairs(Pair[] pairs){
		for(int j=0;j<pairs.length;j++){
			System.err.print(pairs[j].getFirst()+", ");
		}
		System.err.println();
	}
	
	public static void randomSort(Pair[] pairs){
		Random rand=new Random();
		for(int i=0;i<pairs.length;i++){
			int j=rand.nextInt(pairs.length);
			Pair temp=pairs[i];
			pairs[i]=pairs[j];
			pairs[j]=temp;
		}
	}
	
	public static void writeGlobalStat(Coordinator coord){
		String filePath="globalstat.bin";
		GlobalStat gs=new GlobalStat();
		gs.setEventMap(ServiceManager.getDefaultInstance().getEventRegistry().getEventMap());
		gs.setWorkerIdMap(ServiceManager.getDefaultInstance().getWorkerIdMap());
		gs.setContainerNameMap(coord.costEval.containerNameMap);
		gs.setContainerStatMap(coord.costEval.containerStatMap);
		gs.setProcWorkerStatMap(coord.costEval.procWorkerStatMap);
		gs.setGateWorkerStatMap(coord.costEval.gateWorkerStatMap);
		gs.setContainerTreeMap(coord.containerTreeMap);
		gs.setContainerIdMap(coord.containerIdMap);
		
		KryoFileWriter.writeToFile(gs, filePath);
//		GlobalStat.writeToFile(ServiceManager.getDefaultInstance().getMappedEventRegistry().getEventMap(), 
//				ServiceManager.getDefaultInstance().getWorkerMetaMap(), 
//				coord.costEval.containerMap, coord.costEval.containerStatMap, filePath);
//		String filePath="eventmap.bin";
//		String filePath="eventprop.bin";
//		KryoFileWriter.writeToFile(ServiceManager.getDefaultInstance().getMappedEventRegistry().getEventMap().get("A").getProperty("name"), filePath);
	}
	
	public static void sleep(long timeMS){
		try {Thread.sleep(timeMS);} catch(InterruptedException e1){e1.printStackTrace();}
	}
	
	class Pair extends Tuple2D<Integer,String>{
		public Pair(Integer first, String second) {
			super(first, second);
		}
	}
}
