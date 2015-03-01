package dist.esper.monitor.ui;

import org.eclipse.swt.widgets.Composite;

import dist.esper.io.GlobalStat;
import java.util.concurrent.locks.*;

public abstract class AbstractMonitorComposite {
	protected GlobalStat gs;
	protected Composite parent;
	protected ReentrantLock lock=new ReentrantLock();
	
	public abstract void update(GlobalStat gs);
	public abstract Composite getComposite();
}
