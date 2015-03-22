package dist.esper.monitor.ui.data;

import java.text.SimpleDateFormat;

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
import org.jfree.ui.GradientPaintTransformType;
import org.jfree.ui.StandardGradientPaintTransformer;

import dist.esper.core.cost.WorkerStat;

public class WorkerStatData{
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
		TimeSeries cpuFreeTS=new TimeSeries(CPU_FREE);
		cpuSet.addSeries(cpuUsageTS);
		cpuSet.addSeries(cpuFreeTS);
		
		memSet=new TimeSeriesCollection();
		TimeSeries memUsedTS=new TimeSeries(MEM_USED);
		TimeSeries memFreeTS=new TimeSeries(MEM_FREE);
		memSet.addSeries(memUsedTS);
		memSet.addSeries(memFreeTS);
		
		netSet=new TimeSeriesCollection();
		TimeSeries bwUsageTs=new TimeSeries(BW_USAGE);
		TimeSeries bwFreeTs=new TimeSeries(BW_FREE);
		netSet.addSeries(bwUsageTs);
		netSet.addSeries(bwFreeTs);
		
		subpubCountSet = new DefaultCategoryDataset();
		oprCountSet = new DefaultCategoryDataset();
		timeSet = new DefaultCategoryDataset();
		
		Integer zero=Integer.valueOf(0);
		subpubCountSet.setValue(zero, LOCAL_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		subpubCountSet.setValue(zero, REMOTE_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		subpubCountSet.setValue(zero, LOCAL_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		subpubCountSet.setValue(zero, REMOTE_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		
		oprCountSet.setValue(zero, FILTER_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(zero, FILTER_DELAYED_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(zero, PATTERN_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(zero, PATTERN_DELAYED_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(zero, JOIN_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(zero, JOIN_DELAYED_COUNT, OPR_COUNT_CATEGORY);
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
				"percentage",//CPU_USAGE, 
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
				"MB", //MEM_USED,
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
				"percentage",//BW_USAGE, 
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
	    //localNumberAxis.setRange(0.0D, 100.0D);
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
	    //localNumberAxis.setRange(0.0D, 100.0D);
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
	    //localNumberAxis.setRange(0.0D, 100.0D);
	    
	    localNumberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
	    BarRenderer localBarRenderer = (BarRenderer)localCategoryPlot.getRenderer();

	    localBarRenderer.setGradientPaintTransformer(new StandardGradientPaintTransformer(GradientPaintTransformType.HORIZONTAL));
	    localBarRenderer.setDrawBarOutline(false);
	    localBarRenderer.setLegendItemToolTipGenerator(new StandardCategorySeriesLabelGenerator("Tooltip: {0}"));
	}
	
	/*should be wrapper in a Runable*/
	public void update(WorkerStat ws){
		cpuSet.getSeries(CPU_USAGE).add(new Millisecond(), ws.cpuUsage);//FIXME
		cpuSet.getSeries(CPU_FREE).add(new Millisecond(), 1.0-ws.cpuUsage);//FIXME
		memSet.getSeries(MEM_USED).add(new Millisecond(), ws.memUsed/1E6);//FIXME
		memSet.getSeries(MEM_FREE).add(new Millisecond(), ws.memFree/1E6);//FIXME
		netSet.getSeries(BW_USAGE).add(new Millisecond(), ws.bwUsageUS/1E6);//FIXME
		netSet.getSeries(BW_FREE).add(new Millisecond(), 1.0-ws.bwUsageUS/1E6);//FIXME
		
		subpubCountSet.setValue(ws.localSubscriberCount, LOCAL_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		subpubCountSet.setValue(ws.remoteSubscriberCount, REMOTE_SUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		subpubCountSet.setValue(ws.localPublisherCount, LOCAL_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		subpubCountSet.setValue(ws.remotePublisherCount, REMOTE_PUB_COUNT, SUB_PUB_COUNT_CATEGORY);
		
		oprCountSet.setValue(ws.filterCount, FILTER_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(ws.filterDelayedCount, FILTER_DELAYED_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(ws.patternCount, PATTERN_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(ws.patternDelayedCount, PATTERN_DELAYED_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(ws.joinCount, JOIN_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(ws.joinDelayedCount, JOIN_DELAYED_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(ws.rootCount, ROOT_COUNT, OPR_COUNT_CATEGORY);
		oprCountSet.setValue(ws.getRawStreamSampleCount(), RAW_STAT_COUNT, OPR_COUNT_CATEGORY);
		
		timeSet.setValue(ws.filterCondProcTimeUS, FILTER_COND_PROC_TIME, TIME_CATEGORY);
		timeSet.setValue(ws.joinCondProcTimeUS, JOIN_COND_PROC_TIME, TIME_CATEGORY);
		timeSet.setValue(ws.sendBaseTimeUS, SEND_BASE_TIME, TIME_CATEGORY);
		timeSet.setValue(ws.sendByteRateUS, SEND_BYTE_RATE, TIME_CATEGORY);
	}
	
	public static final String CPU_USAGE="cpu usage";
	public static final String CPU_FREE="cpu free";
	public static final String MEM_USED="mem used";
	public static final String MEM_FREE="mem free";
	public static final String BW_USAGE="bandwidth usage";
	public static final String BW_FREE="bandwidth free";
	
	public static final String SUB_PUB_COUNT_CATEGORY="numbers of subscriber and publisher";
	public static final String LOCAL_SUB_COUNT="local subscriber";
	public static final String REMOTE_SUB_COUNT="remote subscriber";
	public static final String LOCAL_PUB_COUNT="local publisher";
	public static final String REMOTE_PUB_COUNT="remote publisher";
	
	public static final String OPR_COUNT_CATEGORY="numbers of operators";
	public static final String FILTER_COUNT="filter";
	public static final String FILTER_DELAYED_COUNT="filter delayed";
	public static final String PATTERN_COUNT="pattern";
	public static final String PATTERN_DELAYED_COUNT="pattern delayed";
	public static final String JOIN_COUNT="join";
	public static final String JOIN_DELAYED_COUNT="join delayed";
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

