package dist.esper.monitor.ui.old;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.StandardCategorySeriesLabelGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.experimental.chart.swt.ChartComposite;
import org.jfree.ui.GradientPaintTransformType;
import org.jfree.ui.StandardGradientPaintTransformer;

import dist.esper.core.cost.WorkerStat;
import dist.esper.core.id.WorkerId;
import dist.esper.io.GlobalStat;
import dist.esper.monitor.ui.AbstractMonitorComposite;

@Deprecated
public class StatComposite extends AbstractMonitorComposite{
//	GlobalStat gs;
//	Composite parent;
	Composite comp;
	Composite fgComps[][];
	
	int width0;
	int height0;
	
	ScrolledComposite sc;
	
	//Map<String,Integer> wmriMap=new HashMap<String,Integer>();
	//List<WorkerMeta> wmList=new ArrayList<WorkerMeta>();
	List<WorkerStatData> wsdList=new ArrayList<WorkerStatData>();
	
	static int CPU_COLUMN_INDEX=0;
	static int MEM_COLUMN_INDEX=1;
	static int NET_COLUMN_INDEX=2;
	static int SUB_PUB_COUNT_COLUMN_INDEX=3;
	static int OPR_COUNT_COLUMN_INDEX=4;
	static int TIME_COLUMN_INDEX=5;
	
	static final String ROW="row";
	static final String COL="col";
	
	public StatComposite(Composite parent, int width0, int height0) {
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
		layout.numColumns = TIME_COLUMN_INDEX+1;
		comp.setLayout(layout);
		sc.setContent(comp);
		comp.pack();
	}
	
	public void reinitFigureComposites(){
		if(fgComps!=null){
			for(int i=0;i<fgComps.length;i++){
				for(int j=0;j<fgComps[i].length;j++){
					fgComps[i][j].dispose();
				}
			}
			wsdList.clear();
		}
		fgComps=new Composite[gs.getWorkerIdMap().size()][];
		for(int i=0;i<fgComps.length;i++){
			fgComps[i]=new Composite[TIME_COLUMN_INDEX+1];
			for(int j=0;j<=TIME_COLUMN_INDEX;j++){
				fgComps[i][j]=new Composite(comp, SWT.NONE);
				fgComps[i][j].setData(ROW, i);
				fgComps[i][j].setData(COL, j);
				fgComps[i][j].setLayout(new FillLayout());
				fgComps[i][j].setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));
				
				GridData gd=new GridData();
				gd.minimumHeight=200;
				gd.minimumWidth=250;
				gd.heightHint=gd.minimumHeight;
				gd.widthHint=gd.minimumWidth;
				fgComps[i][j].setLayoutData(gd);
			}
		}
		
		int i=0;
		for(WorkerId wm: gs.getWorkerIdMap().values()){
			WorkerStatData wsd=new WorkerStatData(wm.getId());
			wsd.init();
			initChartComposite(wsd.getCpuChart(), fgComps[i][CPU_COLUMN_INDEX]);
			initChartComposite(wsd.getMemChart(), fgComps[i][MEM_COLUMN_INDEX]);
			initChartComposite(wsd.getNetChart(), fgComps[i][NET_COLUMN_INDEX]);
			initChartComposite(wsd.getSubPubCountChart(), fgComps[i][SUB_PUB_COUNT_COLUMN_INDEX]);
			initChartComposite(wsd.getOprCountChart(), fgComps[i][OPR_COUNT_COLUMN_INDEX]);
			initChartComposite(wsd.getTimeChart(), fgComps[i][TIME_COLUMN_INDEX]);
			wsdList.add(wsd);
			i++;
		}
		comp.pack();
	}

	public static ChartComposite initChartComposite(JFreeChart chart, Composite fgComp){
		ChartComposite chartComp = null;
		chartComp = new ChartComposite(fgComp, SWT.NONE, chart, true);
		chartComp.setDisplayToolTips(true);
		chartComp.setHorizontalAxisTrace(false);
		chartComp.setVerticalAxisTrace(false);
		//chartComp.setSize(width0, height0);
		return chartComp;
	}
	
	//UpdateCompositeRunnable upCompRun=new UpdateCompositeRunnable();
	@Override
	public void update(GlobalStat gs){
		this.lock.lock();
		GlobalStat oldGS=this.gs;
		this.gs=gs;
		if(oldGS==null || oldGS.getWorkerIdMap().size()!=this.gs.getWorkerIdMap().size()){
			this.reinitFigureComposites();
		}
		for(WorkerStatData wsd: wsdList){
			WorkerStat ws=gs.getProcWorkerStatMap().get(wsd.getWorkerId());
			ws=(ws!=null)?ws:gs.getGateWorkerStatMap().get(wsd.getWorkerId());
			wsd.update(ws);
		}
		this.lock.unlock();
//		parent.getDisplay().syncExec(upCompRun);
	}
	
//	class UpdateCompositeRunnable implements Runnable{		
//		@Override
//		public void run() {
//			for(WorkerStatData wsd: wsdList){
//				WorkerStat ws=gs.getProcWorkerStatMap().get(wsd.getWorkerId());
//				ws=(ws!=null)?ws:gs.getGateWorkerStatMap().get(wsd.getWorkerId());
//				wsd.update(ws);
//			}
//		}
//	}
	
	class WorkerStatData{
		String workerId;
		TimeSeriesCollection cpuSet;
		TimeSeriesCollection memSet;
		TimeSeriesCollection netSet;
		DefaultCategoryDataset subpubCountSet;
		DefaultCategoryDataset oprCountSet;
		DefaultCategoryDataset timeSet;
		
		JFreeChart cpuChart;
		JFreeChart memChart;
		JFreeChart netChart;
		JFreeChart subpubCountChart;
		JFreeChart oprCountChart;
		JFreeChart timeChart;
		
		public WorkerStatData(String workerId) {
			super();
			this.workerId = workerId;			
		}
		
		public void init(){
			cpuSet=new TimeSeriesCollection();
			TimeSeries cpuUsageTS=new TimeSeries(CPU_USAGE);
			cpuSet.addSeries(cpuUsageTS);
			
			memSet=new TimeSeriesCollection();
			TimeSeries memUsedTS=new TimeSeries(MEM_USED);
			memSet.addSeries(memUsedTS);
			
			netSet=new TimeSeriesCollection();
			TimeSeries bwUsageTs=new TimeSeries(BW_USAGE);
			netSet.addSeries(bwUsageTs);
			
			subpubCountSet = new DefaultCategoryDataset();
			oprCountSet = new DefaultCategoryDataset();
			timeSet = new DefaultCategoryDataset();
			
			Integer zero=Integer.valueOf(0);
			subpubCountSet.setValue(zero, LOCAL_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			subpubCountSet.setValue(zero, REMOTE_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			subpubCountSet.setValue(zero, LOCAL_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			subpubCountSet.setValue(zero, REMOTE_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			
			oprCountSet.setValue(zero, FILTER_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(zero, FILTER_COMP_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(zero, PATTERN_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(zero, PATTERN_COMP_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(zero, JOIN_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(zero, JOIN_COMP_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(zero, ROOT_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(zero, RAW_STAT_COUNT, OPR_COUNT_CATEGORY);
			
			timeSet.setValue(zero, FILTER_COND_PROC_TIME, TIME_CATEGORY);
			timeSet.setValue(zero, JOIN_COND_PROC_TIME, TIME_CATEGORY);
			timeSet.setValue(zero, SEND_BASE_TIME, TIME_CATEGORY);
			timeSet.setValue(zero, SEND_BYTE_RATE, TIME_CATEGORY);
			
			initCPUChart();
			initMemChart();
			initNetChart();
			initSubPubCountChart();
			initOprCountChart();
			initTimeChart();
		}
		
		public void initCPUChart(){
			cpuChart = ChartFactory.createTimeSeriesChart(
					CPU_USAGE, 
					"S", 
					CPU_USAGE, 
					cpuSet, 
					true, true, false);
		    XYPlot localXYPlot = (XYPlot)cpuChart.getPlot();
		    localXYPlot.setDomainPannable(true);
		    localXYPlot.setRangePannable(false);
		    localXYPlot.setDomainCrosshairVisible(true);
		    localXYPlot.setRangeCrosshairVisible(true);
		    XYItemRenderer r = localXYPlot.getRenderer();
	        if (r instanceof XYLineAndShapeRenderer) {
	            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
	            renderer.setBaseShapesVisible(true);
	            renderer.setBaseShapesFilled(true);
	        }
	        
	        DateAxis axis = (DateAxis) localXYPlot.getDomainAxis();
	        axis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss"));
		}
		
		public void initMemChart(){
			memChart = ChartFactory.createTimeSeriesChart(
					MEM_USED, 
					"Seconds",
					MEM_USED, 
					memSet, 
					true, true, false);
		    XYPlot localXYPlot = (XYPlot)memChart.getPlot();
		    localXYPlot.setDomainPannable(true);
		    localXYPlot.setRangePannable(false);
		    localXYPlot.setDomainCrosshairVisible(true);
		    localXYPlot.setRangeCrosshairVisible(true);
		    XYItemRenderer r = localXYPlot.getRenderer();
	        if (r instanceof XYLineAndShapeRenderer) {
	            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
	            renderer.setBaseShapesVisible(true);
	            renderer.setBaseShapesFilled(true);
	        }
	        
	        DateAxis axis = (DateAxis) localXYPlot.getDomainAxis();
	        axis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss"));
		}
		
		public void initNetChart(){
			netChart = ChartFactory.createTimeSeriesChart(
					BW_USAGE, 
					"Seconds",
					BW_USAGE, 
					netSet, 
					true, true, false);
		    XYPlot localXYPlot = (XYPlot)netChart.getPlot();
		    localXYPlot.setDomainPannable(true);
		    localXYPlot.setRangePannable(false);
		    localXYPlot.setDomainCrosshairVisible(true);
		    localXYPlot.setRangeCrosshairVisible(true);
		    XYItemRenderer r = localXYPlot.getRenderer();
	        if (r instanceof XYLineAndShapeRenderer) {
	            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
	            renderer.setBaseShapesVisible(true);
	            renderer.setBaseShapesFilled(true);
	        }
	        
	        DateAxis axis = (DateAxis) localXYPlot.getDomainAxis();
	        axis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss"));
		}
		
		public void initSubPubCountChart(){
			subpubCountChart = ChartFactory.createBarChart(
					SUB_PUB_COUNT_CATEGORY, 
					"category", 
					SUB_PUB_COUNT_CATEGORY, 
					subpubCountSet);
		    CategoryPlot localCategoryPlot = (CategoryPlot)subpubCountChart.getPlot();
		    localCategoryPlot.setOrientation(PlotOrientation.HORIZONTAL);
		    localCategoryPlot.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);
		    NumberAxis localNumberAxis = (NumberAxis)localCategoryPlot.getRangeAxis();
		    localNumberAxis.setRange(0.0D, 100.0D);
		    localNumberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		    BarRenderer localBarRenderer = (BarRenderer)localCategoryPlot.getRenderer();

		    localBarRenderer.setGradientPaintTransformer(new StandardGradientPaintTransformer(GradientPaintTransformType.HORIZONTAL));
		    localBarRenderer.setDrawBarOutline(false);
		    localBarRenderer.setLegendItemToolTipGenerator(new StandardCategorySeriesLabelGenerator("Tooltip: {0}"));
		}
		
		public void initOprCountChart(){
			oprCountChart = ChartFactory.createBarChart(
					OPR_COUNT_CATEGORY, 
					"category", 
					OPR_COUNT_CATEGORY, 
					oprCountSet);
		    CategoryPlot localCategoryPlot = (CategoryPlot)oprCountChart.getPlot();
		    localCategoryPlot.setOrientation(PlotOrientation.HORIZONTAL);
		    localCategoryPlot.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);
		    NumberAxis localNumberAxis = (NumberAxis)localCategoryPlot.getRangeAxis();
		    localNumberAxis.setRange(0.0D, 100.0D);
		    localNumberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		    BarRenderer localBarRenderer = (BarRenderer)localCategoryPlot.getRenderer();

		    localBarRenderer.setGradientPaintTransformer(new StandardGradientPaintTransformer(GradientPaintTransformType.HORIZONTAL));
		    localBarRenderer.setDrawBarOutline(false);
		    localBarRenderer.setLegendItemToolTipGenerator(new StandardCategorySeriesLabelGenerator("Tooltip: {0}"));
		}
		
		public void initTimeChart(){
			timeChart = ChartFactory.createBarChart(
					TIME_CATEGORY, 
					"category", 
					TIME_CATEGORY, 
					timeSet);
		    CategoryPlot localCategoryPlot = (CategoryPlot)timeChart.getPlot();
		    localCategoryPlot.setOrientation(PlotOrientation.HORIZONTAL);
		    localCategoryPlot.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);
		    NumberAxis localNumberAxis = (NumberAxis)localCategoryPlot.getRangeAxis();
		    localNumberAxis.setRange(0.0D, 100.0D);
		    localNumberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
		    BarRenderer localBarRenderer = (BarRenderer)localCategoryPlot.getRenderer();

		    localBarRenderer.setGradientPaintTransformer(new StandardGradientPaintTransformer(GradientPaintTransformType.HORIZONTAL));
		    localBarRenderer.setDrawBarOutline(false);
		    localBarRenderer.setLegendItemToolTipGenerator(new StandardCategorySeriesLabelGenerator("Tooltip: {0}"));
		}
		
		/*should be wrapper in a Runable*/
		public void update(WorkerStat ws){
			cpuSet.getSeries(CPU_USAGE).add(new Millisecond(), ws.cpuUsage);//FIXME
			memSet.getSeries(MEM_USED).add(new Millisecond(), ws.memUsed);//FIXME
			netSet.getSeries(BW_USAGE).add(new Millisecond(), ws.bwUsageUS/1E6);//FIXME
			
			subpubCountSet.setValue(ws.localSubscriberCount, LOCAL_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			subpubCountSet.setValue(ws.remoteSubscriberCount, REMOTE_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			subpubCountSet.setValue(ws.localPublisherCount, LOCAL_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			subpubCountSet.setValue(ws.remotePublisherCount, REMOTE_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
			
			oprCountSet.setValue(ws.filterCount, FILTER_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(ws.filterCompCount, FILTER_COMP_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(ws.patternCount, PATTERN_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(ws.patternCompCount, PATTERN_COMP_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(ws.joinCount, JOIN_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(ws.joinCompCount, JOIN_COMP_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(ws.rootCount, ROOT_COUNT, OPR_COUNT_CATEGORY);
			oprCountSet.setValue(ws.rawStatsCount, RAW_STAT_COUNT, OPR_COUNT_CATEGORY);
			
			timeSet.setValue(ws.filterCondProcTimeUS, FILTER_COND_PROC_TIME, TIME_CATEGORY);
			timeSet.setValue(ws.joinCondProcTimeUS, JOIN_COND_PROC_TIME, TIME_CATEGORY);
			timeSet.setValue(ws.sendBaseTimeUS, SEND_BASE_TIME, TIME_CATEGORY);
			timeSet.setValue(ws.sendByteRateUS, SEND_BYTE_RATE, TIME_CATEGORY);
		}
		
		public static final String CPU_USAGE="cpu usage";
		public static final String MEM_USED="mem used";
		public static final String BW_USAGE="bandwidth usage";
		
		public static final String SUB_PUB_COUNT_CATEGORY="numbers of subscriber and publisher";
		public static final String LOCAL_SUB_COUNT="local subscriber";
		public static final String REMOTE_SUB_COUNT="remote subscriber";
		public static final String LOCAL_PUB_COUNT="local publisher";
		public static final String REMOTE_PUB_COUNT="remote publisher";
		
		public static final String OPR_COUNT_CATEGORY="numbers of operators";
		public static final String FILTER_COUNT="filter";
		public static final String FILTER_COMP_COUNT="filter compatible";
		public static final String PATTERN_COUNT="pattern";
		public static final String PATTERN_COMP_COUNT="pattern compatible";
		public static final String JOIN_COUNT="join";
		public static final String JOIN_COMP_COUNT="join compatible";
		public static final String ROOT_COUNT="root";
		public static final String RAW_STAT_COUNT="raw stat count";
		
		public static final String TIME_CATEGORY="time of conditions and sending";
		public static final String FILTER_COND_PROC_TIME="filter condition processing time(US)";
		public static final String JOIN_COND_PROC_TIME="join condition processing time(US)";
		public static final String SEND_BASE_TIME="send base time(US)";
		public static final String SEND_BYTE_RATE="send byte rate per US";
		
		public String getWorkerId() {
			return workerId;
		}

		public JFreeChart getCpuChart() {
			return cpuChart;
		}

		public JFreeChart getMemChart() {
			return memChart;
		}

		public JFreeChart getNetChart() {
			return netChart;
		}

		public JFreeChart getSubPubCountChart() {
			return subpubCountChart;
		}

		public JFreeChart getOprCountChart() {
			return oprCountChart;
		}
		
		public JFreeChart getTimeChart() {
			return timeChart;
		}
	}
	
	public void printLocation(){
		System.out.format("comp: %d, %d, %d, %d\n",comp.getLocation().x, comp.getLocation().y , 
				comp.getSize().x, comp.getSize().y);
		for(int i=0;i<fgComps.length;i++){
			for(int j=0;j<fgComps[i].length;j++){
				Composite subComp=fgComps[i][j];
				System.out.format("fgComp: %d-%d,  %d, %d, %d, %d\n",
						subComp.getData(ROW), subComp.getData(COL),
						subComp.getLocation().x, subComp.getLocation().y , 
						subComp.getSize().x, subComp.getSize().y);
			}
		}
	}
}

