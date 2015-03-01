package dist.esper.monitor.test;


import org.eclipse.nebula.widgets.nattable.examples.AbstractNatExample;
import org.eclipse.nebula.widgets.nattable.examples.runner.StandaloneNatExampleRunner;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

import dist.esper.core.flow.container.StreamContainer;
import dist.esper.core.id.WorkerId;
import dist.esper.io.GlobalStat;
import dist.esper.io.KryoFileWriter;
import dist.esper.monitor.ui.custom.InstanceTable;
import dist.esper.monitor.ui.data.ContainerListWrapper;

public class TestInstanceTable extends AbstractNatExample {
	GlobalStat gs;
	public static void main(String[] args) {
		String filePath="globalstat.bin";
		GlobalStat gs=(GlobalStat)KryoFileWriter.readFromFile(filePath);
		
		TestInstanceTable test=new TestInstanceTable(gs);
        StandaloneNatExampleRunner.run(800, 400, test);
    }
	
	public TestInstanceTable(GlobalStat gs) {
		this.gs = gs;
	}
	
	@Override
	public Control createExampleControl(Composite parent) {
		InstanceTable it=new InstanceTable(parent);
		it.init();
		
		ContainerListWrapper clw=new ContainerListWrapper(new WorkerId("worker","localhost",8000), null);
		for(StreamContainer sc: gs.getContainerNameMap().values()){
			clw.addStreamContainer(sc);
		}
		
		it.update(clw);
		return it.getNatTable();
	}

}
