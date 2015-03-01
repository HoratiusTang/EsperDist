package dist.esper.test;

import java.util.*;

import org.apache.commons.configuration.ConfigurationException;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import com.espertech.esper.epl.spec.StatementSpecCompiled;

import dist.esper.core.util.ServiceManager;
import dist.esper.epl.expr.StatementSpecification;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.epl.sementic.StatementVisitor;
import dist.esper.event.Event;
import dist.esper.event.EventRegistry;
import dist.esper.proxy.EPAdministratorImplProxy;

public class TestStatementObjectModel {
	
	public static void main(String[] args){
		try {
			ServiceManager.initConfig(args);
		} catch (ConfigurationException e1) {
			e1.printStackTrace();
		}
		Configuration config = new Configuration();
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);
		EPAdministratorImplProxy epAdminProxy=new EPAdministratorImplProxy(epService.getEPAdministrator());
		
		List<Event> eventList=genEvents();
		
		for(Event event: eventList){
			epService.getEPAdministrator().getConfiguration().addEventType(event.getName(), event);
			ServiceManager.getDefaultInstance().getEventRegistry().registEvent(event);
		}
		
		String epl00, epl20;
		epl00="select b00.id as t, b00.name as t from B(id+2>3 or price>6.0).win:time(5 sec) as b00";
		epl20="select count(*), a20.id, a20.time+3 , avg(b20.id, b20.id>8), UDFClass.doubleName(b20.name), b20.clientIds[0] " +
				"from A(id*2>5).win:time(1 min 6 sec) as a20, B(id>3).win:time(5 sec) as b20 " +
				"where (not a20.id=b20.id) and a20.time<b20.time " +
				"group by b20.name, b20.id " + 
				"having a20.price>5 " +
				"order by a20.id desc " +
				"limit 10";
		
		String[] epls=new String[]{epl20};
		
		for(int i=0;i<epls.length;i++){
			String epl=epls[i];
			try{
				EPStatementObjectModel som=epService.getEPAdministrator().compileEPL(epl);
				StatementSpecification ss=StatementSpecification.Factory.make(som);				
				StatementVisitor sv=new StatementVisitor(i, ServiceManager.getDefaultInstance().getEventRegistry());
				sv.visitStatementSpecification(ss);
				System.out.println(ss.toString());
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
		for(int i=0;i<epls.length;i++){
			String epl=epls[i];
			StatementSpecCompiled compiled=epAdminProxy.compileQuery(epl);
			StatementSpecification ss=StatementSpecification.Factory.make(compiled);
			StatementSementicWrapper ssw=new StatementSementicWrapper(i, ServiceManager.getDefaultInstance().getEventRegistry());
			try {
				ss.resolve(ssw, null);
				System.out.println(ss.toString());
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static List<Event> genEvents(){
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
		
		List<Event> eventList=new ArrayList<Event>();
		eventList.add(a);
		eventList.add(b);
		eventList.add(c);
//		eventList.add(d);
		
		return eventList;
	}
}
