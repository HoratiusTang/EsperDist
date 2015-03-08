package dist.esper.monitor.test;

import dist.esper.io.GlobalStat;
import dist.esper.io.KryoFileWriter;
import dist.esper.monitor.ui.MonitorWindow;

public class TestMonitorWindow {

	public static void main(String[] args) {
		String filePath="globalstat.bin";
		GlobalStat gs=(GlobalStat)KryoFileWriter.readFromFile(filePath);
		MonitorWindow win=new MonitorWindow();
		win.init();
		win.update(gs);
		win.display();
	}

}
