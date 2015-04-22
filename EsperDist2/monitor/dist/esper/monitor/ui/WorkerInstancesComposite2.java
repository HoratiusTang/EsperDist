package dist.esper.monitor.ui;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.nebula.widgets.nattable.selection.command.ClearAllSelectionsCommand;
import org.eclipse.nebula.widgets.nattable.util.GUIHelper;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.zest.core.widgets.Graph;
import org.eclipse.zest.core.widgets.GraphConnection;
import org.eclipse.zest.core.widgets.GraphNode;
import org.eclipse.zest.core.widgets.ZestStyles;
import org.eclipse.zest.layouts.LayoutAlgorithm;
import org.eclipse.zest.layouts.LayoutStyles;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.flow.container.DerivedStreamContainer;
import dist.esper.core.flow.container.FilterDelayedStreamContainer;
import dist.esper.core.flow.container.FilterStreamContainer;
import dist.esper.core.flow.container.JoinDelayedStreamContainer;
import dist.esper.core.flow.container.JoinStreamContainer;
import dist.esper.core.flow.container.RootStreamContainer;
import dist.esper.core.flow.container.StreamContainer;
import dist.esper.core.flow.container.StreamContainerFlow;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.core.id.WorkerId;
import dist.esper.epl.expr.util.BooleanExpressionNoEventAliasStringlizer;
import dist.esper.io.GlobalStat;
import dist.esper.monitor.ui.custom.CGraph;
import dist.esper.monitor.ui.custom.EplTable;
import dist.esper.monitor.ui.custom.InstanceColorFactory;
import dist.esper.monitor.ui.custom.InstanceSelectionListener;
import dist.esper.monitor.ui.custom.InstanceTable;
import dist.esper.monitor.ui.data.ContainerListWrapper;
import dist.esper.monitor.ui.layout.FlowLayoutAlgorithm;
import dist.esper.monitor.ui.layout.GridFixedSizeLayoutAlgorithm;

public class WorkerInstancesComposite2 extends AbstractMonitorComposite {
	//CBanner banner;	
		Composite wiComp;
		ScrolledComposite wiScrollComp;
		//Composite[][] igComps;
		//org.eclipse.swt.widgets.List eplList;
		EplTable eplTable;
		Composite flowGraphComp;
		CGraph flowGraph;	
		//Text flowText;
		
		Table insStatTable;
		
		SashForm wholeSash;
		SashForm botSash;
		
		int width0;
		int height0;
		
		int nodeWidth=40;
		int nodeHeight=30;
		
		int igGraphWidth=60;
		int igGraphHeight=30;
		
		String[] insStatTableColumnNames={"name","value"};

		TreeMap<String,InstanceTable> instanceTableMap=new TreeMap<String,InstanceTable>();
		
		static final String ROW="row";
		static final String COL="col";
		static final String GRAPH="graph";
		static final String UNIQUENAME="uniqueName";
		
		Map<String,GraphNode> nodeMap=new HashMap<String, GraphNode>();
		List<Graph> graphList=new ArrayList<Graph>();
		Map<String,Integer> wmriMap=new HashMap<String,Integer>();
		LayoutAlgorithm flowLayoutAlgorithm=new FlowLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING);
		FlowGraphListener flowGraphListener=new FlowGraphListener();
		
		public WorkerInstancesComposite2(Composite parent, int width0, int height0) {
			super();		
			this.parent = parent;
			this.width0 = width0;
			this.height0 = height0;
		}
		
		public Composite getComposite(){
			return wholeSash;
		}

		public void init(){
			//all
			wholeSash=new SashForm(parent, SWT.VERTICAL);
			//top
			SashForm topSash=new SashForm(wholeSash, SWT.HORIZONTAL);		
			//bottom
			botSash=new SashForm(wholeSash, SWT.HORIZONTAL);
			
			//top.left
			wiScrollComp=new ScrolledComposite(topSash, SWT.H_SCROLL|SWT.V_SCROLL);
			wiScrollComp.setLayout(new FillLayout());
			wiComp=new Composite(wiScrollComp, SWT.FILL);
			wiComp.setSize(width0, height0);
//			wiComp.setLayout(new FillLayout());
			GridLayout layout = new GridLayout();
			layout.numColumns=1;			
			wiComp.setLayout(layout);
			wiScrollComp.setContent(wiComp);

			//top.right
			eplTable=new EplTable(topSash, SWT.MULTI|SWT.V_SCROLL|SWT.H_SCROLL|SWT.FULL_SELECTION);
			eplTable.getTable().addMouseListener(eplTableListener);

			Composite flowGraphComp=new Composite(botSash, SWT.NONE);
			flowGraphComp.setLayout(new FillLayout());
			flowGraph=new CGraph(flowGraphComp, SWT.NONE);
			flowGraph.setConnectionStyle(ZestStyles.CONNECTIONS_DIRECTED);

			//bottom.right
			//flowText=new Text(botSash, SWT.NONE);
			initInstanceStatTable(botSash);
			
			//set weights
			topSash.setWeights(new int[]{3,1});
			botSash.setWeights(new int[]{3,1});
			wholeSash.setWeights(new int[]{3,1});
		}
		
		public void initInstanceStatTable(Composite container){
			insStatTable=new Table(container, SWT.H_SCROLL|SWT.V_SCROLL|SWT.MULTI|SWT.FULL_SELECTION);
			insStatTable.setLayout(new FillLayout());
			insStatTable.setHeaderVisible(true);
			insStatTable.setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));
			
			for(int i=0;i<insStatTableColumnNames.length;i++){
				TableColumn col=new TableColumn(insStatTable, SWT.NONE);
				col.setText(insStatTableColumnNames[i]);
			}
			insStatTable.pack();
		}
		
		/**
		 * 
		 * @param instanceName may be name of DerivedStreamContainer or RawStream
		 */
		public void updateInstanceStatTable(String instanceName){
			insStatTable.removeAll();
			InstanceStat insStat=gs.getContainerStatMap().get(instanceName);
			
			if(insStat!=null){
				DerivedStreamContainer dsc=(DerivedStreamContainer)gs.getContainerNameMap().get(instanceName);
				if(dsc!=null){
					addInstanceStatTableItem("unique name",insStat.getUniqueName());
					addInstanceStatTableItem("type",insStat.getType());
					addInstanceStatTableItem("worker id",insStat.getWorkerId());
					addInstanceStatTableItem("condition",BooleanExpressionNoEventAliasStringlizer.getInstance().toString(dsc.getOwnCondition()));
					addInstanceStatTableItem("duration",insStat.durationUS()/1000000 + " s");
					addInstanceStatTableItem("event count",insStat.getEventCount());
					addInstanceStatTableItem("processing time",insStat.getProcTimeUS()/100000 + " s");
					addInstanceStatTableItem("output interval",insStat.getOutputIntervalUS() + " us");
					addInstanceStatTableItem("output rate",insStat.getOutputRateSec() + " /s");
				}
			}
			else{
				RawStreamStat rawStat=gs.getRawStreamStatMap().get(instanceName);
				addInstanceStatTableItem("event name",rawStat.getEventName());
				addInstanceStatTableItem("duration",rawStat.durationUS()/1000000 + " s");
				addInstanceStatTableItem("event count",rawStat.getEventCount());
				addInstanceStatTableItem("output rate",rawStat.getOutputRateSec() + " /s");
			}
			
			for(TableColumn col: insStatTable.getColumns()){
				col.pack();//MUST, shit
			}
			insStatTable.pack();
		}
		
		private TableItem addInstanceStatTableItem(Object name, Object value){
			TableItem item=new TableItem(insStatTable, SWT.NONE);
			item.setText(new String[]{name.toString(), value.toString()});
			return item;
		}
		
		@Override
		public void update(GlobalStat gs){
			this.lock.lock();
			GlobalStat oldGS=this.gs;
			this.gs=gs;
			if(oldGS==null || oldGS.getWorkerIdMap().size()!=this.gs.getWorkerIdMap().size()){
				this.reinitWorkerInstanceComposite();
			}
			updateInstanceTables();
			updateEplTable();
			this.lock.unlock();
		}
		
		public void updateEplTable(){
			eplTable.getTable().removeAll();
			for(StreamContainerFlow sct: gs.getContainerTreeMap().values()){
				TableItem item=eplTable.addItem(sct.getEplId(), sct.getEpl());
				item.setData(sct);
			}
			eplTable.pack();
		}
		
		public void reinitWorkerInstanceComposite(){
			for(WorkerId wm: gs.getWorkerIdMap().values()){
				if(!instanceTableMap.containsKey(wm.getId())){
					Composite composite=new Composite(wiComp, SWT.NONE);
					InstanceTable instanceTable=new InstanceTable(composite, instanceSelectionHandler);
					//InstanceTable instanceTable=new InstanceTable(wiComp, instanceSelectionHandler);
					instanceTableMap.put(wm.getId(), instanceTable);
					instanceTable.init();
				}
			}
		}
		InstanceSelectionHandler instanceSelectionHandler=new InstanceSelectionHandler();
		class InstanceSelectionHandler implements InstanceSelectionListener{
			@Override
			public void instanceCellSelected(String instanceName,  InstanceTable instanceTable) {
				System.out.println(instanceName);
				ClearAllSelectionsCommand clearCmd=new ClearAllSelectionsCommand();
				for(InstanceTable it: instanceTableMap.values()){
					if(it!=instanceTable){
						it.doCommand(clearCmd);
						it.refresh();
					}
				}
				instanceTableCellSelected(instanceName);
			}
		}
//		public InstanceGroupGraphListener igGraphlistener=new InstanceGroupGraphListener();
		public void updateInstanceTables(){
			Map<String,ContainerListWrapper> workerIdToClwMap=new TreeMap<String,ContainerListWrapper>();
			for(WorkerId wm: gs.getWorkerIdMap().values()){
				workerIdToClwMap.put(wm.getId(), new ContainerListWrapper(wm, gs.getContainerStatMap()));
			}
			for(StreamContainer sc: gs.getContainerNameMap().values()){
				ContainerListWrapper clw=workerIdToClwMap.get(sc.getWorkerId().getId());
				clw.addStreamContainer(sc);
			}
			
			for(WorkerId wm: gs.getWorkerIdMap().values()){
				ContainerListWrapper clw=workerIdToClwMap.get(wm.getId());
				InstanceTable it=this.instanceTableMap.get(wm.getId());
				it.update(clw);
			}
			wiComp.pack();
			wiScrollComp.pack();
		}
		
		private Color getInstanceColor(Stream stream){
			if(stream instanceof RawStream){
				RawStreamStat rawStat=gs.getRawStreamStatMap().get(stream.getUniqueName());
				return InstanceColorFactory.getRawStreamColor(rawStat);
			}
			else{
				InstanceStat insStat=gs.getContainerStatMap().get(stream.getUniqueName());
				return InstanceColorFactory.getInstanceColor(insStat);
			}
		}
		
		public void instanceTableCellSelected(String instanceName){
			if(instanceName==null){
				return;
			}
			DerivedStreamContainer dsc=gs.getContainerNameMap().get(instanceName);
		
			clearGraph(flowGraph);
			if(dsc==null){
				return;
			}
			
			GraphNode selfNode=newFlowGraphNode(dsc);			
			for(Stream upStream: dsc.getUpStreams()){
				GraphNode upNode=newFlowGraphNode(upStream);				
				new GraphConnection(flowGraph, SWT.ARROW, upNode, selfNode); 
			}
			for(Long downId: dsc.getDownContainerIdList()){
				DerivedStreamContainer downStream=gs.getContainerIdMap().get(downId);
				if(downStream==null){ return; }
				GraphNode downNode=newFlowGraphNode(downStream);				
				new GraphConnection(flowGraph, SWT.ARROW, selfNode, downNode);
			}
			flowGraph.removeMouseListener(flowGraphListener);
			flowGraph.setLayoutAlgorithm(flowLayoutAlgorithm, true);
			flowGraph.addMouseListener(flowGraphListener);
			
			updateInstanceStatTable(instanceName);
		}
		
		private GraphNode newFlowGraphNode(Stream stream){
			GraphNode node=new GraphNode(flowGraph, SWT.ALPHA);
			String text=getFlowGraphNodeText(stream);
			node.setText(text);
			node.setData(stream.getUniqueName());
			Color color=getInstanceColor(stream);
			node.setBackgroundColor(color);	
			node.setBorderColor(color);
			node.setHighlightColor(color);
			node.setBorderHighlightColor(GUIHelper.COLOR_BLACK);
			flowGraph.setData(stream.getUniqueName(), node);
			return node;
		}
		
		private String getFlowGraphNodeText(Stream stream){
			if(stream instanceof RawStream){
				RawStream rawStream=(RawStream)stream;
				return rawStream.getEventName();
			}
			else{
				DerivedStreamContainer dsc=(DerivedStreamContainer)stream;
				String condStr=BooleanExpressionNoEventAliasStringlizer.getInstance().toString(dsc.getOwnCondition());
				return dsc.getUniqueName()+"("+dsc.getDownContainerIdList().size()+")"+"\n"+condStr;
			}
		}
		
		public void flowGraphNodeSelected(GraphNode node){
			this.updateInstanceStatTable((String)node.getData());
		}
		
		public void flowGraphNodeDoubleClicked(GraphNode node){
			DerivedStreamContainer psc=gs.getContainerNameMap().get(node.getData());
			if(psc==null){
				return;
			}
			for(Long downId: psc.getDownContainerIdList()){
				DerivedStreamContainer downStream=gs.getContainerIdMap().get(downId);
				if(downStream==null){ return; }
				if(flowGraph.getData(downStream.getUniqueName())==null){
					GraphNode downNode=newFlowGraphNode(downStream);
					new GraphConnection(flowGraph, SWT.ARROW, node, downNode);
				}
			}
			flowGraph.removeMouseListener(flowGraphListener);
			flowGraph.setLayoutAlgorithm(flowLayoutAlgorithm, true);
			flowGraph.addMouseListener(flowGraphListener);
		}
		
		public void eplTableItemDoubleClicked(TableItem item){
			StreamContainerFlow sct=(StreamContainerFlow)item.getData();
			RootStreamContainer rsc=sct.getRootContainer();
			clearGraph(flowGraph);
			createFlowGraphRecursively(rsc);
			eplTable.getTable().removeMouseListener(eplTableListener);
			flowGraph.setLayoutAlgorithm(flowLayoutAlgorithm, true);
			flowGraph.addMouseListener(flowGraphListener);
			eplTable.getTable().addMouseListener(eplTableListener);
		}
		
		public GraphNode createFlowGraphRecursively(Stream sc){
			GraphNode curNode=newFlowGraphNode(sc);
			if(sc instanceof RootStreamContainer){
				RootStreamContainer rsc=(RootStreamContainer)sc;
				GraphNode childNode=createFlowGraphRecursively(rsc.getUpContainer());
				new GraphConnection(flowGraph, SWT.ARROW, childNode, curNode);
			}		
			else if(sc instanceof JoinDelayedStreamContainer){
				JoinDelayedStreamContainer jcsc=(JoinDelayedStreamContainer)sc;
				GraphNode agentNode=createFlowGraphRecursively(jcsc.getAgent());
				new GraphConnection(flowGraph, SWT.ARROW, agentNode, curNode);
			}
			else if(sc instanceof JoinStreamContainer){
				JoinStreamContainer jsc=(JoinStreamContainer)sc;
				for(StreamContainer csc: jsc.getUpContainerList()){
					GraphNode childNode=createFlowGraphRecursively(csc);
					new GraphConnection(flowGraph, SWT.ARROW, childNode, curNode);
				}
			}
			else if(sc instanceof FilterDelayedStreamContainer){
				FilterDelayedStreamContainer fcsc=(FilterDelayedStreamContainer)sc;
				GraphNode agentNode=createFlowGraphRecursively(fcsc.getAgent());
				new GraphConnection(flowGraph, SWT.ARROW, agentNode, curNode);
			}
			else if(sc instanceof FilterStreamContainer){			
				FilterStreamContainer fsc=(FilterStreamContainer)sc;
				GraphNode rawNode=createFlowGraphRecursively(fsc.getRawStream());
				new GraphConnection(flowGraph, SWT.ARROW, rawNode, curNode);
			}
			else if(sc instanceof RawStream){
				/** do nothing **/
			}
			return curNode;
		}
		
		public static void clearGraph(Graph graph){		
			List<?> nodes=graph.getNodes();
			List<GraphNode> nodeList=new ArrayList<GraphNode>(nodes.size());
			for(Object node: nodes){
				nodeList.add((GraphNode)node);
			}
			for(int i=0;i<nodeList.size();i++){
				nodeList.get(i).dispose();
			}
			nodeList=null;
			if(graph instanceof CGraph){
				((CGraph)graph).removeAllData();
			}
		}
		
		class FlowGraphListener implements MouseListener{
			@Override public void mouseDown(MouseEvent e) {
				System.out.println(e);
				assert(e.getSource()==flowGraph);
				List<?> selection = ((Graph) e.widget).getSelection();
				if(selection.size()==1){
					GraphNode node=(GraphNode)selection.get(0);
					flowGraphNodeSelected(node);
				}
			}
			@Override public void mouseUp(MouseEvent e) {}

			@Override
			public void mouseDoubleClick(MouseEvent e) {
				System.out.println(e);
				assert(e.getSource()==flowGraph);
				List<?> selection = ((Graph) e.widget).getSelection();
				if(selection.size()==1){
					GraphNode node=(GraphNode)selection.get(0);
					flowGraphNodeDoubleClicked(node);
				}
			}
		}
		
//		class InstanceGroupGraphListener implements SelectionListener{
//			@Override
//			public void widgetSelected(SelectionEvent e) {
//				System.out.println(e);
//				Graph graph=(Graph)e.widget;
//				List<?> selection = ((Graph) e.widget).getSelection();
//				if(selection.size()==1){
//					GraphNode node=(GraphNode)selection.get(0);
//					for(Graph g: graphList){
//						if(g!=graph){
//							g.setSelection(null);
//						}
//					}
//					InstanceGraphNodeSelected(graph, node);
//				}
//			}
//			@Override
//			public void widgetDefaultSelected(SelectionEvent e) {}
//		}
		
		EplTableListener eplTableListener=new EplTableListener();
		class EplTableListener implements MouseListener{
			@Override public void mouseDown(MouseEvent e) {}
			@Override public void mouseUp(MouseEvent e) {}

			@Override
			public void mouseDoubleClick(MouseEvent e) {
				System.out.println(e);
				assert(e.getSource()==flowGraph);
				//List<?> selection = ((Table) e.widget).getS
				TableItem[] items=((Table) e.widget).getSelection();
				if(items.length==1){
					TableItem item=items[0];
					StreamContainerFlow sct=(StreamContainerFlow)item.getData();
					System.out.format("table item eplId=%d\n", sct.getEplId());//TODO
					eplTableItemDoubleClicked(item);
				}
			}
		}
		
//		public static int getColumnIndex(DerivedStreamContainer psc){
//			if(psc instanceof FilterStreamContainer){
//				return FILTER_COLUMN_INDEX;
//			}
//			else if(psc instanceof FilterDelayedStreamContainer){
//				return FILTER_COMPATIBLE_COLUMN_INDEX;
//			}
//			else if(psc instanceof JoinStreamContainer){
//				return JOIN_COLUMN_INDEX;
//			}
//			else if(psc instanceof JoinDelayedStreamContainer){
//				return JOIN_COMPATIBLE_COLUMN_INDEX;
//			}
//			else if(psc instanceof RootStreamContainer){
//				return ROOT_COLUMN_INDEX;
//			}
//			return -1;
//		}
		
		public void printLocation(){
//			System.out.format("comp: %d, %d, %d, %d\n",wiComp.getLocation().x, wiComp.getLocation().y , 
//					wiComp.getSize().x, wiComp.getSize().y);
//			for(int i=0;i<igComps.length;i++){
//				for(int j=0;j<igComps[i].length;j++){
//					Composite subComp=igComps[i][j];
//					System.out.format("subcomp: %d-%d,  %d, %d, %d, %d\n",
//							subComp.getData(ROW), subComp.getData(COL),
//							subComp.getLocation().x, subComp.getLocation().y , 
//							subComp.getSize().x, subComp.getSize().y);
//				}
//			}
		}

}
