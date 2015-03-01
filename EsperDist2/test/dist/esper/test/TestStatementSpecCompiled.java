package dist.esper.test;

import java.util.ArrayList;
import java.util.List;


import com.espertech.esper.client.*;
import com.espertech.esper.epl.expression.*;
import com.espertech.esper.epl.spec.*;
import com.espertech.esper.pattern.*;

import dist.esper.core.coordinator.Coordinator;
import dist.esper.core.flow.centralized.Tree;
import dist.esper.core.flow.centralized.TreeBuilder;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.ServiceManager;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.proxy.EPAdministratorImplProxy;

public class TestStatementSpecCompiled {
	public static void main(String[] args){
		Configuration config = new Configuration();
		config.addEventTypeAutoName("org.myapp.event");
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);
		EPAdministratorImplProxy epa=new EPAdministratorImplProxy(epService.getEPAdministrator());;
		
//		EventRegistry er=ServiceManager.getEventRegistry();
//		er.registPackageByName("org.myapp.event");
//		System.out.println(er.toString());
		test1(epa);
	}
	
	public static void test1(EPAdministratorImplProxy epa){
		try{
			//String epl = "select max1(price), avg(price1) from OrderEvent.swin:time(5 sec)";
			String epl=null;
			//epl = "select clientId[1], b, b1, a[0].ids[2] as a0ids2, a[0].id as a0id, count(distinct a[0].id), count(*, b.price<50.0 and b.id>10), rate(b), avg(b.price,b.id>10)/avg(b.price), sum(a[0].price), UDFClass.getInstance(a[0].id, UDFClass.doubleName(a[0].name)).hash(a[1].name) as h from " +
			
			epl = "select avg(b.price,b.id>10)/avg(b.price) from " +
			 
//			epl = "select count(*, b.price<50.0 and b.id>10) from " +
//					"select *, b1 as bb, c, a2[1] ,a[2].id as a0id, count(distinct a[0].id), count(*, b.price<50.0 and b.id>10), rate(b), avg(b.price,b.id>10)/avg(b.price), sum(a[0].price), UDFClass.getInstance(a[0].id, UDFClass.doubleName(a[0].name)).hash(a[1].name) as h from " +
//					"pattern [every [2]b1=B], "+
//					"pattern [every b2=B where timer:withinmax(3,2)], "+
//					"pattern [every timer:interval(3)->b3=B], "+
//					"pattern [((every a2=A) while(a2.id>2)) where timer:withinmax(4,2)], " +
//					"pattern [(every timer:interval(2 year 3 day 10 sec)) " +
//					"and ((every b=B) where timer:withinmax(2 sec,2)) " +
//					"-> [2](a=A(a.id>b.id))].win:length(5).std:firstevent(), " +
//					"pattern [every aa=org.myapp.event.A((price>100.0 and aa.name!=\"apple\" and id<=20) or price<50.0)].win:time(2)" +
//					"pattern [every a=A(a.id>a.price and a.price*2<a.id)] "+	

					"pattern [(every [2](a=A where timer:withinmax(4,2)) -> b1=B(a[0].id>ids[1]+3)) -> every [7](every[2](c=C(c.id>b1.id and c.id<a[1].id))-> c2=C(c2.id>b1.id))], "+
					"B.win:length(6) as b "+
					//"B as b "+
					"where b.name=\"beer\" and (a[0].price>a[1].price or a[0].name = \"apple\") " +
					"group by b.name " +
					"having b.price>10.0 " +
					"output every 10 sec " +
					"order by b.id asc, a[1].price desc " +
					"limit 5";
//			
//			epl = "select sum(A.price), B.id from A(A.id>5 and id>price).win:keepall() as a, B(id>3 or id*2<price).win:length(5) where A.id=B.id group by B.id";
			//epl = "select  b.clientId, a, b[2] from pattern [[2]b=B].win:time(5), A.win:time(5) as c";
			//epl = "select b[0].clientId, time, a, b[1] from pattern [[2]b=B].win:time(5), A.win:time(5) as a";
			//epl = "select time from pattern [[2]b=B].win:time(5), A.win:time(5)";
			//epl = "select b[1] from pattern [[2]b=B].win:time(5), A.win:time(5) as a";
			//epl = "select clientId from A.win:length(5), B.win:length(5) as b";
//			epl = "select * from pattern [a=A -> (b=B -> c=C)].win:length(5) as p, B(id>3).win:length(2) as b2, C(id<2).win:time(4 min) as c " +
//					"where b2.name='aaa' and a.id=b2.id";
			//epl = "select * from B(id>3).win:length(2) as b2";
			
			epl = "select a, a[1].id, b.id, c2.id, sum(b.clientId) " +
					"from pattern [every([2](a=A) -> b=B -> c=C)], B(id>3).win:length(5) as b2, C(price>14.0).win:time(6) as c2 " +
					"where a[0].id=b.id and b2.id=b.id and b2.id>c2.id and b2.name=c2.name";
			
			//StatementSementicWrapper ssw=new StatementSementicWrapper(ServiceManager.getEventRegistry());
			
			StatementSpecCompiled compiled=epa.compileQuery(epl);
			
			StatementSpecification ss=StatementSpecification.Factory.make(compiled);
			
			//ss.resolve(ssw, null);
			
			System.out.println(ss.toString());
			
			TreeBuilder builder=new TreeBuilder(ss.getEplId(), epl, ss);
			builder.buildTreeList();
			List<Tree> treeList=builder.getTreeList();
			
			for(Tree tree: treeList){
				System.out.println(tree.toString());
			}			
			
			System.out.println();
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
}
