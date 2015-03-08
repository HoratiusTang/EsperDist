package dist.esper.test;

import java.util.*;

import org.apache.commons.configuration.ConfigurationException;

import dist.esper.core.CoordinatorMain;
import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.Options;
import dist.esper.core.util.ServiceManager;
import dist.esper.core.worker.Worker;
import dist.esper.event.Event;
import dist.esper.event.EventRegistry;
import dist.esper.external.Spout;
import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.external.event.FieldGenerator;
import dist.esper.external.event.FieldGeneratorFactory;
import dist.esper.io.GlobalStat;
import dist.esper.io.KryoFileWriter;
import dist.esper.util.Logger2;
import dist.esper.util.Tuple2D;

public class TestSimulation2 {
	static Logger2 log=Logger2.getLogger(TestSimulation2.class);
	//List<Event> eventList=new ArrayList<Event>();
	List<EventInstanceGenerator> eventGenList=new ArrayList<EventInstanceGenerator>(4);
	List<Worker> workerList=new ArrayList<Worker>();
	List<Spout> spoutList=new ArrayList<Spout>();
	Coordinator coord=null;
	public static void main(String[] args){
		TestSimulation2 sim=new TestSimulation2();
		try {
			sim.init(args);
			sim.run();
		}
		catch (ConfigurationException e) {			
			e.printStackTrace();
		}
	}
	
	public void init(String[] args) throws ConfigurationException{
		ServiceManager.initConfig(args);
		ServiceManager.coordinatorId=
			new WorkerId(ServiceManager.getCoordinatorId(),"127.0.0.1",1000);
		WorkerId[] workerIds=new WorkerId[]{
			new WorkerId("worker1","127.0.0.1",1001),
			new WorkerId("worker2","127.0.0.1",1002),
			new WorkerId("worker3","127.0.0.1",1003),
			new WorkerId("worker4","127.0.0.1",1004),
			new WorkerId("worker5","127.0.0.1",1005),
		};
		
		for(WorkerId wm: workerIds){
			ServiceManager.getInstance(wm.getId()).registerWorkerId(wm);//register itself
		}
		
		coord=new Coordinator(ServiceManager.getCoordinatorId());
		for(WorkerId wm: workerIds){
			Worker worker=new Worker(wm.getId());
			workerList.add(worker);
		}
		
		coord.init();
		for(Worker w: workerList){
			w.init();
		}
		
		/**genEvents();
		for(int i=0;i<eventList.size();i++){
			String id="spout"+(i+1);
			Event event=eventList.get(i);
			WorkerMeta spoutMeta=new WorkerMeta(id,"127.0.0.1",2001+i);
			spoutMeta.setSpout();
			ServiceManager.getInstance(spoutMeta.getId()).registerWorkerMeta(spoutMeta);//register itself
			Spout spout=new Spout(id, event);
			
			spoutList.add(spout);
			spout.init();		
		}
		*/
		genEventInstanceGenerators();
		for(int i=0;i<eventGenList.size();i++){
			String id="spout"+(i+1);
			WorkerId spoutId=new WorkerId(id,"127.0.0.1",2001+i);
			spoutId.setSpout();
			ServiceManager.getInstance(spoutId.getId()).registerWorkerId(spoutId);//register itself
			
			Spout spout=new Spout(id, eventGenList.get(i));
			spoutList.add(spout);
			spout.init();
		}
	}
	
	private void genEventInstanceGenerators(){
		String[] eventNames={"A","B","C","D"};
		for(String eventName: eventNames){
			EventInstanceGenerator eventGen=genEventInstanceGenerator(eventName);
			eventGenList.add(eventGen);
		}
	}
	
	private static EventInstanceGenerator genEventInstanceGenerator(String eventName){
		Event e=new Event(eventName);
		e.addProperty("id", Integer.class);
		e.addProperty("age", Integer.class);
		e.addProperty("name", String.class);
		e.addProperty("price", Double.class);
		e.addProperty("time", Long.class);
		e.addProperty("clientIds", int[].class);
		
		Map<String,FieldGenerator> fgMap=new HashMap<String,FieldGenerator>();
		fgMap.put("id", new FieldGeneratorFactory.IntegerMonotoGenerator(0, 1));
		fgMap.put("age", new FieldGeneratorFactory.IntegerNormalGenerator(40, 1, 1, 100));
		fgMap.put("name", new FieldGeneratorFactory.StringRandomChooser(new String[]{"Alice","Bob","Cleve","David","Elvis","Fedon","Glora","Harry","Illis"}));
		fgMap.put("price", new FieldGeneratorFactory.DoubleUniformGenerator(10, 100));
		fgMap.put("time", new FieldGeneratorFactory.LongNormalGenerator(10, 1, 1, 30));
		fgMap.put("clientIds", new FieldGeneratorFactory.IntArrayGenerator(10));
		
		EventInstanceGenerator eventGen=new EventInstanceGenerator(e, fgMap);
		return eventGen;
	}
	
	/**
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
	*/
	public void run(){
		coord.start(false);
		for(Spout spout: spoutList){
			spout.start(false);
		}
		for(Worker w: workerList){
			w.start(false);
		}
		String epl00=null, epl01=null, epl02=null, epl03=null, epl04=null, epl05=null, epl06=null, epl07=null, epl08=null, epl09=null;
		String epl10=null, epl11=null, epl12=null, epl13=null, epl14=null, epl15=null, epl16=null, epl17=null, epl18=null, epl19=null;
		String epl20=null, epl21=null, epl22=null, epl23=null, epl24=null, epl25=null, epl26=null, epl27=null, epl28=null, epl29=null;
		String epl30=null, epl31=null, epl32=null, epl33=null, epl34=null, epl35=null, epl36=null, epl37=null, epl38=null, epl39=null;
		String epl40=null, epl41=null, epl42=null, epl43=null, epl44=null, epl45=null, epl46=null, epl47=null, epl48=null, epl49=null;
		epl00="select b00.id, b00.price from B(id>3).win:time(5 sec) as b00";
		epl01="select b01.id, b01.name from B(id>3 or price>6.0).win:time(5 sec) as b01";
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
		
		epl30="select a30.id, a30.clientIds, b30.id, b30.name, b30.clientIds, c30.price " +
				"from A(id>5).win:time(6 sec) as a30, B(id>3).win:time(5 sec) as b30, C(id>=6).win:time(5 sec) as c30 " +
				"where a30.id=b30.id and b30.price>c30.price";
		
		epl31="select a31.id, a31.clientIds, a31, b31.id, b31.name, b31.clientIds, c31.price " +
				"from A(id>5).win:time(6 sec) as a31, B(id>3).win:time(5 sec) as b31, C(id>=6).win:time(5 sec) as c31 " +
				"where a31.id=b31.id and b31.price>c31.price";
		
		epl40="select a40.id, a40.clientIds, b40.id, b40.name, b40.clientIds, c40.price " +
				"from A(id>5).win:time(6 sec) as a40, B(id>3).win:time(5 sec) as b40, C(id>=6).win:time(5 sec) as c40, A(id>=6).win:time(5 sec) as a40_2 " +
				"where a40.id=b40.id and b40.price>c40.price and a40_2.time=a40.time";
		
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
				new Pair(30,epl30),
				new Pair(31,epl31),
				new Pair(32,epl32),
				new Pair(33,epl33),
				new Pair(34,epl34),
				new Pair(35,epl35),
				new Pair(36,epl36),
				new Pair(37,epl37),
				new Pair(38,epl38),
				new Pair(39,epl39),
		};
		
		sleep(5000);
		//randomSort(eplPairs);
		//eplPairs=new Pair[]{};
//		eplPairs=new Pair[]{new Pair(00,epl00), new Pair(02,epl02), new Pair(03,epl03), new Pair(04,epl04),
//				new Pair(10,epl10), new Pair(11,epl11), new Pair(12,epl12), new Pair(13,epl13), new Pair(14,epl14),
//				new Pair(20,epl20), new Pair(21, epl21), eplPairs[22]};
//		eplPairs=new Pair[]{eplPairs[02], eplPairs[21], eplPairs[22]};
//		eplPairs=new Pair[]{new Pair(22,epl22)};
		//eplPairs=new Pair[]{new Pair(01,epl01), new Pair(11,epl11), new Pair(20,epl20), new Pair(22,epl22), new Pair(30, epl30)};
//		eplPairs=new Pair[]{new Pair(30,epl30)};
//		eplPairs=new Pair[]{new Pair(31,epl31)};
//		eplPairs=new Pair[]{new Pair(40,epl40)};
//		eplPairs=new Pair[]{new Pair(01,epl01), new Pair(02,epl01)};
		int eplId=-1;
		try {
			for(int i=0;i<eplPairs.length;i++){
				if(eplPairs[i]!=null && eplPairs[i].getSecond()!=null){
					eplId=eplPairs[i].getFirst();
					System.err.println(eplId);
					printPairs(eplPairs);
					if(eplId==22){
						System.out.print("");
					}
					coord.executeEPL(eplPairs[i].getSecond());
					sleep(5000);
				}
			}
			writeGlobalStat(coord);
			System.out.print("");
		}
		catch (Exception e) {
			log.info("stopped at "+eplId);
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
		gs.setRawStreamStatMap(coord.costEval.rawStats.getRawStreamStatMap());
		gs.setContainerNameMap(coord.costEval.containerNameMap);
		gs.setContainerStatMap(coord.costEval.containerStatMap);
		gs.setProcWorkerStatMap(coord.costEval.procWorkerStatMap);
		gs.setGateWorkerStatMap(coord.costEval.gateWorkerStatMap);
		gs.setContainerTreeMap(coord.containerTreeMap);
		gs.setContainerIdMap(coord.containerIdMap);
		
		KryoFileWriter.writeToFile(gs, filePath);
		log.info("write finish");
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
