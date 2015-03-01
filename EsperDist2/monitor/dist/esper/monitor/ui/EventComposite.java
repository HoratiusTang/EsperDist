package dist.esper.monitor.ui;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;

import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.TreeListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.jface.*;
import org.eclipse.jface.viewers.*;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.cost.RawStreamStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.container.DerivedStreamContainer.StreamAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.Stream;
import dist.esper.core.message.SubmitEplResponse;
import dist.esper.core.util.ServiceManager;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.EventPropertySpecification;
import dist.esper.epl.expr.util.DeepCloneReplaceFactory;
import dist.esper.epl.expr.util.DeepReplaceFactory;
import dist.esper.epl.expr.util.EventAliasDumper;
import dist.esper.epl.expr.util.EventOrPropertySpecReferenceDumper;
import dist.esper.event.Event;
import dist.esper.event.EventProperty;
import dist.esper.io.GlobalStat;
import dist.esper.monitor.ui.custom.EplTable;
import dist.esper.util.CollectionUtils;
import dist.esper.util.MultiValueMap;
import dist.esper.util.StringUtil;

import static dist.esper.core.util.ServiceManager.format;

@Deprecated
public class EventComposite extends AbstractMonitorComposite{
//	GlobalStat gs;	
//	Composite parent;
	
	//TreeViewer eventTree;
	Composite eventComp;
	//StyledText text;	
	Tree eventTree;
	
	Table eventStatTable;
	Table eplStatTable;
	SashForm wholeSash;
	SashForm middleSash;
	SashForm topSash;
	
	EplTable eplTable;
	
	Text eplText;
	Button submitBtn;
	
	Text infoText;
	//Composite downComp;
	SubmitEplHook submitEplHook;
	
	String[] eventStatTableColumnNames=new String[]{
			"Property Name",
			"Operator Type",
			"Conditions",
			"Output Rate",
			"Select Factor",
	};
	
	String[] eplStatTableColumnNames=new String[]{
			"Operator Type",
			"Conditions",
			"Output Rate",
			"Select Factor"
	};
	DeepCloneReplaceFactory cloneReplaceFactory=new DeepCloneReplaceFactory();
	int width0;
	int height0;
	
	public EventComposite(Composite parent, int width0, int height0) {
		super();		
		this.parent = parent;
		this.width0 = width0;
		this.height0 = height0;
	}
	
	@Override
	public Composite getComposite(){
		return wholeSash;
	}
	
	public void setSubmitEplHook(SubmitEplHook submitEplHook) {
		this.submitEplHook = submitEplHook;
	}

	@Override
	public void update(GlobalStat gs){
		this.lock.lock();
		this.gs=gs;
		this.updateTree();
		this.updateEplTable();
		this.lock.unlock();
		topSash.layout();
		wholeSash.layout();
	}
	
	public void init(){
		//all
		wholeSash=new SashForm(parent, SWT.VERTICAL|SWT.BORDER);
		//top
		topSash=new SashForm(wholeSash, SWT.HORIZONTAL|SWT.BORDER);
		
		middleSash=new SashForm(wholeSash, SWT.HORIZONTAL|SWT.BORDER);
		
		
		
		SashForm eplTextBtnSash=new SashForm(wholeSash, SWT.HORIZONTAL|SWT.BORDER);
		eplText=new Text(eplTextBtnSash, SWT.MULTI);
		submitBtn=new Button(eplTextBtnSash, SWT.BORDER|SWT.CENTER);
		submitBtn.setText("submit epl");
		
		infoText=new Text(wholeSash, SWT.MULTI);
		
		//top.left		
		eventTree=new Tree(topSash, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
		eventTree.setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));		
		eventTree.addSelectionListener(new TreeSelectionListener());
		
		//top.right
		initEventStatTable(topSash);
		
		//middle.left
		eplTable=new EplTable(middleSash,SWT.V_SCROLL|SWT.H_SCROLL|SWT.FULL_SELECTION);
		eplTable.getTable().addSelectionListener(new EplTableSelectionListener());
		initEplStatTable(middleSash);		
		
		topSash.setWeights(new int[]{1,3});
		middleSash.setWeights(new int[]{1,1});
		eplTextBtnSash.setWeights(new int[]{7,1});
		wholeSash.setWeights(new int[]{14,10,2,3});
		
		submitBtn.addSelectionListener(new SumbitButtonDownListener());
		eventTree.layout();
		eventStatTable.layout();
		topSash.layout();
		wholeSash.layout();
	}
	
	private void initEventStatTable(Composite container){		
		eventStatTable=new Table(container, SWT.H_SCROLL|SWT.V_SCROLL|SWT.MULTI|SWT.FULL_SELECTION);
		eventStatTable.setLayout(new FillLayout());
		eventStatTable.setHeaderVisible(true);
		eventStatTable.setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));
//		eventTable.setLinesVisible(true);
		
//		GridData gd=new GridData();
//		gd.horizontalAlignment=SWT.FILL;
//		gd.grabExcessHorizontalSpace=true;
//		gd.grabExcessVerticalSpace=true;
//		gd.verticalAlignment=SWT.FILL;
//		table.setLayoutData(gd);
		
		for(int i=0;i<eventStatTableColumnNames.length;i++){
			TableColumn col=new TableColumn(eventStatTable, SWT.NONE);
			col.setText(eventStatTableColumnNames[i]);
		}
		eventStatTable.pack();
	}
	
	private void initEplStatTable(Composite container){
		eplStatTable=new Table(container, SWT.H_SCROLL|SWT.V_SCROLL|SWT.MULTI|SWT.FULL_SELECTION);
		eplStatTable.setLayout(new FillLayout());
		eplStatTable.setHeaderVisible(true);
		eplStatTable.setBackground(parent.getDisplay().getSystemColor(SWT.COLOR_WHITE));
//		eplStatTable.setLinesVisible(true);
		
		for(int i=0;i<eplStatTableColumnNames.length;i++){
			TableColumn col=new TableColumn(eplStatTable, SWT.NONE);
			col.setText(eplStatTableColumnNames[i]);
		}		
		eplStatTable.pack();
	}
	
	public void updateTree(){
		if(eventTree.getItems().length == gs.getEventMap().size()){
			return;
		}
		eventTree.removeAll();
		for(Event event: gs.getEventMap().values()){
			createEventTreeItem(event);
		}
		eventTree.pack();
	}
	
	public TreeItem createEventTreeItem(Event event){
		RawStreamStat rawStat=gs.getRawStreamStatMap().get(event.getName());
		TreeItem eventItem=new TreeItem(eventTree, SWT.NULL);
		eventItem.setData(event);
		eventItem.setText(event.getName()+" ["+format(rawStat.getOutputRateSec())+"/s]");
		for(EventProperty prop: event.getPropList()){
			TreeItem propItem=new TreeItem(eventItem, SWT.NULL);
			propItem.setData(prop);
			propItem.setText(prop.getTypeSimpleName()+": "+prop.getName());
		}
		return eventItem;
	}
	
	class SumbitButtonDownListener implements SelectionListener{
		@Override public void widgetDefaultSelected(SelectionEvent e) {}
		
		@Override
		public void widgetSelected(SelectionEvent e) {
			System.out.println(e);
			String epl=eplText.getText().trim();
			if(submitEplHook!=null){
				try {
					SubmitEplResponse serp=submitEplHook.submit(epl);
					if(serp!=null){
						System.out.println(serp.toString());
						if(serp.getEplId()>=0){
							infoText.setText(String.format("submit sucessed with eplId %d", serp.getEplId()));
							eplText.setText("");
						}
						else{
							infoText.setText(String.format("submit failed:\n%s", serp.getInfo()));
						}
						//eplText.pack();
						//infoText.pack();
					}
				}
				catch (Exception e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	private String[] newEmptyTableRow(int numColumns){
		String[] strs=new String[numColumns];
		Arrays.fill(strs, "");
		return strs;
	}
	
	class EplTableSelectionListener implements SelectionListener{
		@Override public void widgetDefaultSelected(SelectionEvent e) {}
		
		@Override
		public void widgetSelected(SelectionEvent e) {
			TableItem item=(TableItem)e.item;
			StreamContainerFlow scf=(StreamContainerFlow)item.getData();
			updateEplEventTable(scf);
		}
	}
	
	public void updateEplEventTable(StreamContainerFlow scf){		
		List<DerivedStreamContainer> dscList=new ArrayList<DerivedStreamContainer>(16);
		dumpOwnAndUpStreamContainersRecursively(scf.getRootContainer(), dscList);
		eplStatTable.removeAll();
		for(DerivedStreamContainer dsc: dscList){
			
			TableItem item=new TableItem(eplStatTable, SWT.NONE);
			String[] cols=newEmptyTableRow(eplStatTableColumnNames.length);
			InstanceStat insStat=gs.getContainerStatMap().get(dsc.getUniqueName());
			cols[0]=dsc.getClass().getSimpleName()+" "+dsc.getId();			
			
			StreamAndMapAndBoolComparisonResult smcr=dsc.getDirectOrIndirectReuseStreamMapComparisonResultByEplId(scf.getEplId());
			Map<EventAlias,EventAlias> streamToContainerEaMap=smcr.getSecond();
			Map<EventAlias,EventAlias> containerToStreamEaMap=CollectionUtils.reverse(streamToContainerEaMap);
			cloneReplaceFactory.setEventAliasReplaceMap(containerToStreamEaMap);
			AbstractBooleanExpression cond=(AbstractBooleanExpression)cloneReplaceFactory.deepClone(dsc.getOwnCondition());
			cols[1]=(cond==null)?"":cond.toString();
			
			cols[2]=format(insStat.getOutputRateSec());
			cols[3]=format(insStat.computeSelectFactor());
			item.setText(cols);
		}
		for(TableColumn col: eplStatTable.getColumns()){
			col.pack();//MUST, shit
		}
		eplStatTable.pack();
	}
	
	private void dumpOwnAndUpStreamContainersRecursively(DerivedStreamContainer dsc, List<DerivedStreamContainer> dscList){
		for(Stream upStream: dsc.getUpStreams()){
			if(upStream instanceof DerivedStreamContainer){
				dumpOwnAndUpStreamContainersRecursively((DerivedStreamContainer)upStream, dscList);
			}
		}
		dscList.add(dsc);
	}
	
	class TreeSelectionListener implements SelectionListener{
		@Override public void widgetDefaultSelected(SelectionEvent e) {}
		@Override
		public void widgetSelected(SelectionEvent e) {
			TreeItem item=(TreeItem)e.item;
			Event event=null;
			EventProperty prop=null;
			if(item.getData() instanceof Event){
				event=(Event)item.getData();
			}
			else if(item.getData() instanceof EventProperty){
				prop=(EventProperty)(item.getData());
				event=prop.getEvent();
			}
			updateEventStatTable(event, prop);
		}
	}
	
	public void updateEventStatTable(Event event, EventProperty selectedProp){
		MultiValueMap<EventProperty, DerivedStreamContainer> propCntsMap=
				new MultiValueMap<EventProperty, DerivedStreamContainer>(propComparator);
		for(DerivedStreamContainer psc: gs.getContainerNameMap().values()){
			AbstractBooleanExpression cond=psc.getOwnCondition();
			//Set<EventOrPropertySpecification> epsSet=cond.dumpAllEventOrPropertySpecReferences();
			if(cond!=null){
				/*Set<EventAlias> eaSet=cond.dumpAllEventAliases();*/
				Set<EventAlias> eaSet=EventAliasDumper.dump(cond);
				boolean flag=false;
				for(EventAlias ea: eaSet){
					if(ea.getEvent().equals(event)){
						flag=true;
						break;
					}
				}
				if(flag){
					/*Set<EventOrPropertySpecification> eopsSet=cond.dumpAllEventOrPropertySpecReferences();*/
					Set<EventOrPropertySpecification> eopsSet=EventOrPropertySpecReferenceDumper.dump(cond);
					for(EventOrPropertySpecification eops: eopsSet){
						if(eops instanceof EventPropertySpecification){
							EventPropertySpecification eps=(EventPropertySpecification)eops;
							if(eps.getEventProp().getEvent().equals(event)){
								propCntsMap.putPair(eps.getEventProp(), psc);
							}
						}
					}
				}
			}
		}
		//setEventText(propCntsMap);
		setEventStatTableContent(propCntsMap, selectedProp);
	}	
	
	private void setEventStatTableContent(MultiValueMap<EventProperty, DerivedStreamContainer> propCntsMap, EventProperty selectedProp){
		eventStatTable.removeAll();
		int selectedPropRowIndex=-1;
		int rowCount=0;
		for(Map.Entry<EventProperty, Set<DerivedStreamContainer>> entry: propCntsMap.entrySet()){
			EventProperty prop=entry.getKey();
			if(selectedProp!=null && selectedProp.equals(prop)){
				selectedPropRowIndex=rowCount;
			}
			Set<DerivedStreamContainer> cntSet=entry.getValue();
			TableItem item=new TableItem(eventStatTable, SWT.NONE); rowCount++;			
			String[] cols=newEmptyTableRow(eventStatTableColumnNames.length);
			cols[0]=prop.fullName();
			item.setText(cols);
			
			for(DerivedStreamContainer psc: cntSet){
				TableItem item2=new TableItem(eventStatTable, SWT.NONE); rowCount++;				
				InstanceStat insStat=gs.getContainerStatMap().get(psc.getUniqueName());
				cols=newEmptyTableRow(eventStatTableColumnNames.length);
				cols[1]=psc.getClass().getSimpleName()+" "+psc.getId();
				AbstractBooleanExpression cond=psc.getOwnCondition();
				cols[2]=(cond==null)?"":cond.toString();
				cols[3]=format(insStat.getOutputRateSec());
				cols[4]=format(insStat.computeSelectFactor());
				item2.setText(cols);
			}
		}
		if(selectedPropRowIndex>=0){
			eventStatTable.select(selectedPropRowIndex);
		}
		for(TableColumn col: eventStatTable.getColumns()){
			col.pack();//MUST, shit
		}
		eventStatTable.pack();
	}
	
	public void updateEplTable(){
		eplTable.getTable().removeAll();
		for(StreamContainerFlow sct: gs.getContainerTreeMap().values()){
			TableItem item=eplTable.addItem(sct.getEplId(), sct.getEpl());
			item.setData(sct);
		}
		eplTable.pack();
	}
	
	EventPropertyComparator propComparator=new EventPropertyComparator();
	class EventPropertyComparator implements Comparator<EventProperty>{
		@Override
		public int compare(EventProperty a, EventProperty b) {
			return a.fullName().compareTo(b.fullName());
		}
	}
}
