package dist.esper.monitor.ui.old;

import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.*;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.*;
import org.eclipse.swt.widgets.*;
import org.eclipse.zest.core.widgets.*;
import org.eclipse.zest.layouts.LayoutAlgorithm;
import org.eclipse.zest.layouts.LayoutStyles;
import org.eclipse.zest.layouts.algorithms.*;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.RawStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.core.id.WorkerId;
import dist.esper.io.*;
import dist.esper.monitor.ui.AbstractMonitorComposite;
import dist.esper.monitor.ui.custom.CGraph;
import dist.esper.monitor.ui.custom.EplTable;
import dist.esper.monitor.ui.layout.FlowLayoutAlgorithm;
import dist.esper.monitor.ui.layout.GridFixedSizeLayoutAlgorithm;

public class WorkerInstancesComposite extends AbstractMonitorComposite{
	//CBanner banner;	
	Composite wiComp;
	Composite[][] igComps;
	//org.eclipse.swt.widgets.List eplList;
	EplTable eplTable;
	Composite flowGraphComp;
	CGraph flowGraph;	
	Text flowText;
	
	SashForm wholeSash;
	SashForm botSash;
	
	int width0;
	int height0;
	
	int nodeWidth=40;
	int nodeHeight=30;
	
	int igGraphWidth=60;
	int igGraphHeight=30;
	
	static int WORKER_INFO_INDEX=0;
	static int FILTER_COLUMN_INDEX=1;
	static int FILTER_COMPATIBLE_COLUMN_INDEX=2;
	static int JOIN_COLUMN_INDEX=3;
	static int JOIN_COMPATIBLE_COLUMN_INDEX=4;
	static int ROOT_COLUMN_INDEX=5;
	
	static final String ROW="row";
	static final String COL="col";
	static final String GRAPH="graph";
	static final String UNIQUENAME="uniqueName";
	
	Map<String,GraphNode> nodeMap=new HashMap<String, GraphNode>();
	List<Graph> graphList=new ArrayList<Graph>();
	Map<String,Integer> wmriMap=new HashMap<String,Integer>();
	LayoutAlgorithm flowLayoutAlgorithm=new FlowLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING);
	FlowGraphListener flowGraphListener=new FlowGraphListener();
	
	public WorkerInstancesComposite(Composite parent, int width0, int height0) {
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
		ScrolledComposite sc=new ScrolledComposite(topSash, SWT.H_SCROLL|SWT.V_SCROLL);
		wiComp=new Composite(sc,SWT.NONE);
		wiComp.setSize(width0, height0);
		GridLayout layout = new GridLayout();
		layout.numColumns = ROOT_COLUMN_INDEX+1;
		wiComp.setLayout(layout);
		sc.setContent(wiComp);

		//top.right
		eplTable=new EplTable(topSash, SWT.MULTI|SWT.V_SCROLL|SWT.H_SCROLL|SWT.FULL_SELECTION);
		eplTable.getTable().addMouseListener(eplTableListener);

		Composite flowGraphComp=new Composite(botSash, SWT.NONE);
		flowGraphComp.setLayout(new FillLayout());
		flowGraph=new CGraph(flowGraphComp, SWT.NONE);
		flowGraph.setConnectionStyle(ZestStyles.CONNECTIONS_DIRECTED);

		//bottom.right
		flowText=new Text(botSash, SWT.NONE);
		
		//set weights
		topSash.setWeights(new int[]{3,1});
		botSash.setWeights(new int[]{3,1});
		wholeSash.setWeights(new int[]{3,1});
	}
	
	@Override
	public void update(GlobalStat gs){
		this.lock.lock();
		GlobalStat oldGS=this.gs;
		this.gs=gs;
		if(oldGS==null || oldGS.getWorkerIdMap().size()!=this.gs.getWorkerIdMap().size()){
			this.reinitWorkerInstanceComposite();
		}
		updateInstanceGroupsContents();
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
		if(igComps!=null){
			for(int i=0;i<igComps.length;i++){
				for(int j=0;j<igComps[i].length;j++){
					igComps[i][j].dispose();
				}
			}
		}
		
		igComps=new Composite[gs.getWorkerIdMap().size()][];
		int i=0;
		for(WorkerId wm: gs.getWorkerIdMap().values()){
			wmriMap.put(wm.getId(), i);
			i++;
		}
		for(i=0;i<igComps.length;i++){
			igComps[i]=new Composite[ROOT_COLUMN_INDEX+1];
			for(int j=0;j<=ROOT_COLUMN_INDEX;j++){
				igComps[i][j]=new Composite(wiComp, SWT.NONE);
				igComps[i][j].setData(ROW, i);
				igComps[i][j].setData(COL, j);
				initInstanceGroupGraphComposite(igComps[i][j]);
			}
		}
	}
	
	public void initInstanceGroupGraphComposite(Composite igComp) {
		GridData gd=new GridData();
		gd.minimumHeight=100;
		gd.minimumWidth=100;
		gd.heightHint=100;
		gd.widthHint=150;
		igComp.setLayoutData(gd);
		
		igComp.setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));

		igComp.setLayout(new FillLayout());
		
		Graph graph = new Graph(igComp, SWT.NONE);
		
		graphList.add(graph);
		graph.addSelectionListener(igGraphlistener);
		
		GridFixedSizeLayoutAlgorithm gfla=new GridFixedSizeLayoutAlgorithm(ZestStyles.NODES_NO_LAYOUT_ANIMATION | LayoutStyles.NO_LAYOUT_NODE_RESIZING);
		gfla.setNodeWidth(nodeWidth);
		gfla.setNodeHeight(nodeHeight);
		graph.setLayoutAlgorithm(gfla, true);

		igComp.setData(GRAPH,graph);
		igComp.layout();
	}
	
	public InstanceGroupGraphListener igGraphlistener=new InstanceGroupGraphListener();
	public void updateInstanceGroupsContents(){
		for(DerivedStreamContainer psc: gs.getContainerNameMap().values()){
			if(nodeMap.containsKey(psc.getUniqueName())){
				continue;
			}
			int row=wmriMap.get(psc.getWorkerId().getId());
			int col=getColumnIndex(psc);
			Composite igComp=igComps[row][col];

			Graph graph=(Graph)igComp.getData(GRAPH);
			GraphNode node = new GraphNode(graph, ZestStyles.NODES_NO_LAYOUT_ANIMATION, psc.getUniqueName());
			
			node.setSize(nodeWidth, nodeHeight);
			node.setData(UNIQUENAME, psc.getUniqueName());
			nodeMap.put(psc.getUniqueName(), node);
		}
		wiComp.pack();
	}
	
	public void InstanceGraphNodeSelected(Graph wiGraph, GraphNode wiNode){
		String uniqueName=(String)wiNode.getData(UNIQUENAME);
		DerivedStreamContainer psc=gs.getContainerNameMap().get(uniqueName);
	
		clearGraph(flowGraph);
		
		GraphNode selfNode=new GraphNode(flowGraph, SWT.ALPHA, psc.getUniqueName());
		selfNode.setData(psc.getUniqueName());
		flowGraph.setData(psc.getUniqueName(), selfNode);
		for(Stream upLocation: psc.getUpStreams()){
			GraphNode upNode=new GraphNode(flowGraph, SWT.ALPHA, upLocation.getUniqueName());
			upNode.setData(upLocation.getUniqueName());
			flowGraph.setData(upLocation.getUniqueName(), upNode);
			new GraphConnection(flowGraph, SWT.ARROW, upNode, selfNode); 
		}
		for(Long downId: psc.getDownContainerIdList()){
			DerivedStreamContainer downPsc=gs.getContainerIdMap().get(downId);
			GraphNode downNode=new GraphNode(flowGraph, SWT.ALPHA, downPsc.getUniqueName());
			downNode.setData(downPsc.getUniqueName());
			flowGraph.setData(downPsc.getUniqueName(), downNode);
			new GraphConnection(flowGraph, SWT.ARROW, selfNode, downNode);
		}
//		flowGraph.layout();
//		flowGraph.setLayoutAlgorithm(new GridLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING), true);
//		flowGraph.setLayoutAlgorithm(new SpringLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING), true);
//		flowGraph.setLayoutAlgorithm(new RadialLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING), true);
//		flowGraph.setLayoutAlgorithm(new TreeLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING), true);
//		flowGraph.setLayoutAlgorithm(new DirectedGraphLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING), true);
//		flowGraph.setLayoutAlgorithm(new HorizontalShift(LayoutStyles.NO_LAYOUT_NODE_RESIZING), true);
//		flowGraph.setLayoutAlgorithm(new HorizontalLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING), true);
		flowGraph.removeMouseListener(flowGraphListener);
		flowGraph.setLayoutAlgorithm(flowLayoutAlgorithm, true);
		flowGraph.addMouseListener(flowGraphListener);
	}
	
	public void flowGraphNodeDoubleClicked(GraphNode node){
		DerivedStreamContainer psc=gs.getContainerNameMap().get(node.getData());
		for(Long downId: psc.getDownContainerIdList()){
			DerivedStreamContainer downPsc=gs.getContainerIdMap().get(downId);
			if(flowGraph.getData(downPsc.getUniqueName())==null){
//				GraphNode downNode=new GraphNode(flowGraph, SWT.ALPHA, downPsc.getUniqueName());
//				downNode.setData(downPsc.getUniqueName());
//				flowGraph.setData(downPsc.getUniqueName(), downNode);
				GraphNode downNode=createGraphNode(downPsc);
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
		eplTable.getTable().addMouseListener(eplTableListener);
	}
	
	public GraphNode createFlowGraphRecursively(Stream sc){
		GraphNode curNode=createGraphNode(sc);
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
	
	private GraphNode createGraphNode(Stream sl){
		GraphNode node=new GraphNode(flowGraph, SWT.ALPHA, sl.getUniqueName());
		node.setData(sl.getUniqueName());
		flowGraph.setData(sl.getUniqueName(), node);
		return node;
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
		@Override public void mouseDown(MouseEvent e) {}
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
	
	class InstanceGroupGraphListener implements SelectionListener{
		@Override
		public void widgetSelected(SelectionEvent e) {
			System.out.println(e);
			Graph graph=(Graph)e.widget;
			List<?> selection = ((Graph) e.widget).getSelection();
			if(selection.size()==1){
				GraphNode node=(GraphNode)selection.get(0);
				for(Graph g: graphList){
					if(g!=graph){
						g.setSelection(null);
					}
				}
				InstanceGraphNodeSelected(graph, node);
			}
		}
		@Override
		public void widgetDefaultSelected(SelectionEvent e) {}
	}
	
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
	
	public static int getColumnIndex(DerivedStreamContainer psc){
		if(psc instanceof FilterStreamContainer){
			return FILTER_COLUMN_INDEX;
		}
		else if(psc instanceof FilterDelayedStreamContainer){
			return FILTER_COMPATIBLE_COLUMN_INDEX;
		}
		else if(psc instanceof JoinStreamContainer){
			return JOIN_COLUMN_INDEX;
		}
		else if(psc instanceof JoinDelayedStreamContainer){
			return JOIN_COMPATIBLE_COLUMN_INDEX;
		}
		else if(psc instanceof RootStreamContainer){
			return ROOT_COLUMN_INDEX;
		}
		return -1;
	}
	
	public void printLocation(){
		System.out.format("comp: %d, %d, %d, %d\n",wiComp.getLocation().x, wiComp.getLocation().y , 
				wiComp.getSize().x, wiComp.getSize().y);
		for(int i=0;i<igComps.length;i++){
			for(int j=0;j<igComps[i].length;j++){
				Composite subComp=igComps[i][j];
				System.out.format("subcomp: %d-%d,  %d, %d, %d, %d\n",
						subComp.getData(ROW), subComp.getData(COL),
						subComp.getLocation().x, subComp.getLocation().y , 
						subComp.getSize().x, subComp.getSize().y);
			}
		}
	}
}
