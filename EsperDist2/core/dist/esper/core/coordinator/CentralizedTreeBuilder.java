package dist.esper.core.coordinator;

import java.util.List;

import com.espertech.esper.client.soda.EPStatementObjectModel;

import dist.esper.core.flow.centralized.*;
import dist.esper.core.util.*;
import dist.esper.epl.expr.StatementSpecification;
import dist.esper.epl.sementic.StatementVisitor;
import dist.esper.util.Logger2;

public class CentralizedTreeBuilder {
	static Logger2 log=Logger2.getLogger(CentralizedTreeBuilder.class);
	Coordinator coordinator;	
	public CentralizedTreeBuilder(Coordinator coordinator) {
		super();
		this.coordinator = coordinator;
	}
	public List<Tree> generateTree(long eplId, String epl) throws Exception{
		EPStatementObjectModel som=coordinator.epService.getEPAdministrator().compileEPL(epl);
		StatementSpecification ss=StatementSpecification.Factory.make(som);				
		StatementVisitor sv=new StatementVisitor(eplId, ServiceManager.getInstance(coordinator.id).getEventRegistry());
		sv.visitStatementSpecification(ss);
		
		log.debug(ss.toString());
		
		TreeBuilder builder=new TreeBuilder(eplId, epl, ss);
		List<Tree> treeList=builder.buildTreeList();
		
		for(Tree tree: treeList){
			log.debug(tree.toString());
		}
		return treeList;
	}
}
