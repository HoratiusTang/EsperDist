package dist.esper.proxy;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.core.service.*;
import com.espertech.esper.epl.parse.*;
import com.espertech.esper.epl.spec.*;

public class EPAdministratorImplProxy{
	public EPAdministratorImpl epa=null;
	public StatementLifecycleSvcImplProxy slv=null;
	
	public EPAdministratorImplProxy(EPAdministrator epa){
		this.epa=(EPAdministratorImpl)epa;
		this.slv=new StatementLifecycleSvcImplProxy((StatementLifecycleSvc)(this.epa.services.getStatementLifecycleSvc()));
	}
	
	public StatementSpecCompiled compileQuery(String epl){
		StatementSpecRaw statementSpec = epa.compileEPLToRaw(epl);
		StatementSpecCompiled compiledSpec=slv.createAndStart(statementSpec, epl, false, null, null, null, null, null);
		return compiledSpec;		
	}
}
