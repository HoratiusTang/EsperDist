package dist.esper.monitor.ui;

import java.util.TreeMap;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.jfree.chart.JFreeChart;
import org.jfree.experimental.chart.swt.ChartComposite;

import dist.esper.core.cost.WorkerStat;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.NumberFormatter;
import dist.esper.io.GlobalStat;
import dist.esper.monitor.ui.data.WorkerStatData;

public class StatComposite2 extends AbstractMonitorComposite{
	ScrolledComposite sc;
	Composite comp;	
	int width0;
	int height0;

	TreeMap<String, WorkerStatData> wsdMap=new TreeMap<String, WorkerStatData>();
	TreeMap<String, WorkerStatComposite> wscMap=new TreeMap<String, WorkerStatComposite>();
	
	static int CPU_COLUMN_INDEX=0;
	static int MEM_COLUMN_INDEX=1;
	static int NET_COLUMN_INDEX=2;
	static int SUB_PUB_COUNT_COLUMN_INDEX=3;
	static int OPR_COUNT_COLUMN_INDEX=4;
	static int TIME_COLUMN_INDEX=5;
	
	static final String ROW="row";
	static final String COL="col";
	
	public StatComposite2(Composite parent, int width0, int height0) {
		super();
		this.parent = parent;
		this.width0 = width0;
		this.height0 = height0;
	}
	
	@Override
	public Composite getComposite(){
		return sc;
	}
	
	public void init(){
		sc=new ScrolledComposite(parent, SWT.H_SCROLL|SWT.V_SCROLL);
		comp=new Composite(sc, SWT.NONE);
		comp.setSize(width0, height0);
		GridLayout layout = new GridLayout();
		layout.numColumns = 1;
		comp.setLayout(layout);
		sc.setContent(comp);
		comp.pack();
	}
	
	public void reinitFigureComposites(){
		for(WorkerId wm: gs.getWorkerIdMap().values()){
			if(!wscMap.containsKey(wm.getId())){
				WorkerStatComposite wsc=new WorkerStatComposite(comp, wm.getId());
				wsc.init();
				wscMap.put(wm.getId(), wsc);
			}
		}
	}
	
	@Override
	public void update(GlobalStat gs){
		this.lock.lock();
		GlobalStat oldGS=this.gs;
		this.gs=gs;
		if(oldGS==null || oldGS.getWorkerIdMap().size()!=this.gs.getWorkerIdMap().size()){
			this.reinitFigureComposites();
		}
		for(WorkerStatComposite wsc: wscMap.values()){
			WorkerStat ws=gs.getProcWorkerStatMap().get(wsc.getWorkerId());
			ws=(ws!=null)?ws:gs.getGateWorkerStatMap().get(wsc.getWorkerId());
			wsc.update(ws);
		}
		comp.pack();
		this.lock.unlock();
	}
	
	class WorkerStatComposite{
		Composite parent;
		String workerId;
		WorkerStatData workerStatData;
		
		Composite composite;
		Composite rowHeaderComp;
		Table workerMetaTable;
		Composite[] chartComps;
		
		final String[] workerMetaTableColumnNames={"name","value"};

		public WorkerStatComposite(Composite parent, String workerId) {
			super();
			this.parent = parent;
			this.workerId = workerId;
		}
		
		public void init(){
			composite=new Composite(parent, SWT.NONE);
			GridLayout layout = new GridLayout();
			layout.numColumns = TIME_COLUMN_INDEX+2;
			composite.setLayout(layout);
			
			initRowHeaderComposite();
			
			chartComps=new Composite[TIME_COLUMN_INDEX+1];
			for(int j=0;j<=TIME_COLUMN_INDEX;j++){
				chartComps[j]=new Composite(composite, SWT.BORDER);
				chartComps[j].setLayout(new FillLayout());
				chartComps[j].setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));
				
				GridData gd=new GridData();
				gd.minimumHeight=220;
				gd.minimumWidth=280;
				gd.heightHint=gd.minimumHeight;
				gd.widthHint=gd.minimumWidth;
				chartComps[j].setLayoutData(gd);
			}
			
			workerStatData=new WorkerStatData(workerId);
			workerStatData.init();
			initChartComposite(workerStatData.getCpuChart(), chartComps[CPU_COLUMN_INDEX]);
			initChartComposite(workerStatData.getMemChart(), chartComps[MEM_COLUMN_INDEX]);
			initChartComposite(workerStatData.getNetChart(), chartComps[NET_COLUMN_INDEX]);
			initChartComposite(workerStatData.getSubPubCountChart(), chartComps[SUB_PUB_COUNT_COLUMN_INDEX]);
			initChartComposite(workerStatData.getOprCountChart(), chartComps[OPR_COUNT_COLUMN_INDEX]);
			initChartComposite(workerStatData.getTimeChart(), chartComps[TIME_COLUMN_INDEX]);
			composite.pack();
		}
		
		public void initRowHeaderComposite(){
			rowHeaderComp=new Composite(composite, SWT.BORDER);
			GridLayout layout = new GridLayout();
			layout.numColumns = 1;
			rowHeaderComp.setLayout(layout);
			rowHeaderComp.setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));
			
			GridData gd=new GridData();
			gd.minimumHeight=200;
			gd.minimumWidth=230;
			gd.heightHint=gd.minimumHeight;
			gd.widthHint=gd.minimumWidth;
			rowHeaderComp.setLayoutData(gd);
			
			Text text=new Text(rowHeaderComp, SWT.SINGLE);
			text.setFont(new Font(composite.getDisplay(),"Verdana",16,SWT.BOLD));
			text.setText(workerId);
			text.setEditable(false);
			text.setEnabled(false);
			
			initWorkerMetaTable();
		}
		
		public void initWorkerMetaTable(){
			workerMetaTable=new Table(rowHeaderComp, SWT.H_SCROLL|SWT.V_SCROLL|SWT.MULTI|SWT.FULL_SELECTION|SWT.BORDER);
			workerMetaTable.setLayout(new FillLayout());
			workerMetaTable.setHeaderVisible(false);
			workerMetaTable.setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));
			
			for(int i=0;i<workerMetaTableColumnNames.length;i++){
				TableColumn col=new TableColumn(workerMetaTable, SWT.NONE);
				col.setText(workerMetaTableColumnNames[i]);
			}
			workerMetaTable.pack();
		}
		
		public void updateWorkerMetaTable(){
			WorkerStat ws=gs.getGateWorkerStatMap().get(workerId);
			if(ws==null){
				ws=gs.getProcWorkerStatMap().get(workerId);
			}
			addWorkerMetaTableItem("type",ws.isGateway()?"gateway":"processing");
			addWorkerMetaTableItem("total mem",NumberFormatter.format((ws.getMemFree()+ws.getMemUsed())/1e6)+" MB");
			addWorkerMetaTableItem("cpu cores",ws.getCpuCoreCount());
			addWorkerMetaTableItem("cpu hz",NumberFormatter.format(ws.getCpuHZ()/1e9)+" GHz");
			addWorkerMetaTableItem("process-threads", ws.getProcThreadCount());
			addWorkerMetaTableItem("publish-threads", ws.getPubThreadCount());
			
			for(TableColumn col: workerMetaTable.getColumns()){
				col.pack();//MUST, shit
			}
			workerMetaTable.pack();
		}
		
		private TableItem addWorkerMetaTableItem(Object name, Object value){
			TableItem item=new TableItem(workerMetaTable, SWT.NONE);
			item.setText(new String[]{name.toString(), value.toString()});
			return item;
		}
		
		public ChartComposite initChartComposite(JFreeChart chart, Composite chartCompContainer){
			ChartComposite chartComp = null;
			chartComp = new ChartComposite(chartCompContainer, SWT.NONE, chart, true);
			chartComp.setDisplayToolTips(true);
			chartComp.setHorizontalAxisTrace(false);
			chartComp.setVerticalAxisTrace(false);		
			return chartComp;
		}
		
		public void update(WorkerStat ws){
			this.workerStatData.update(ws);
			updateWorkerMetaTable();
		}

		public Composite getComposite() {
			return composite;
		}

		public WorkerStatData getWorkerStatData() {
			return workerStatData;
		}

		public void setWorkerStatData(WorkerStatData workerStatData) {
			this.workerStatData = workerStatData;
		}
		
		public String getWorkerId(){
			return workerId;
		}
	}
}

