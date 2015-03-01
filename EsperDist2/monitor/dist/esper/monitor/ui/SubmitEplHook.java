package dist.esper.monitor.ui;

import dist.esper.core.message.SubmitEplResponse;

public interface SubmitEplHook {
	public SubmitEplResponse submit(String epl) throws Exception;
}
