package dist.esper.monitor.ui.custom;


import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.nebula.widgets.nattable.*;
import org.eclipse.nebula.widgets.nattable.command.ILayerCommand;
import org.eclipse.nebula.widgets.nattable.config.*;
import org.eclipse.nebula.widgets.nattable.data.*;
import org.eclipse.nebula.widgets.nattable.grid.GridRegion;
import org.eclipse.nebula.widgets.nattable.grid.data.DefaultCornerDataProvider;
import org.eclipse.nebula.widgets.nattable.grid.layer.*;
import org.eclipse.nebula.widgets.nattable.layer.*;
import org.eclipse.nebula.widgets.nattable.layer.cell.*;
import org.eclipse.nebula.widgets.nattable.layer.event.ILayerEvent;
import org.eclipse.nebula.widgets.nattable.painter.cell.TextPainter;
import org.eclipse.nebula.widgets.nattable.painter.layer.ILayerPainter;
import org.eclipse.nebula.widgets.nattable.selection.SelectionLayer;
import org.eclipse.nebula.widgets.nattable.selection.command.ClearAllSelectionsCommand;
import org.eclipse.nebula.widgets.nattable.selection.config.DefaultSelectionLayerConfiguration;
import org.eclipse.nebula.widgets.nattable.selection.event.CellSelectionEvent;
import org.eclipse.nebula.widgets.nattable.style.theme.ModernNatTableThemeConfiguration;
import org.eclipse.nebula.widgets.nattable.ui.menu.HeaderMenuConfiguration;
import org.eclipse.nebula.widgets.nattable.util.GUIHelper;
import org.eclipse.nebula.widgets.nattable.viewport.ViewportLayer;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;

import dist.esper.core.cost.InstanceStat;
import dist.esper.core.flow.container.*;
import dist.esper.core.util.NumberFormatter;
import dist.esper.monitor.ui.data.ContainerListWrapper;

public class InstanceTable {
	ReentrantLock lock=new ReentrantLock();
	Composite parent;
	
	ContainerListWrapper clw;
	
	String[] typeHeaders={"Filter","Filter Delayed", "Join", "Join Delayed", "Root"};
	
	List<StreamContainer>[] sortedCntLists=(List<StreamContainer>[]) Array.newInstance(List.class, typeHeaders.length);
	int colWidth=60;
	int rowHeight=40;
	int colSpanPerType=3;
	int rowCount=1;
	
	LayerListener layerListener=new LayerListener();
	InstanceSelectionListener instanceSelectionListener=null;
	DodyDataProvider bodyDataProvider;
	DataLayer bodyDataLayer;
	SelectionLayer selectionLayer;
	ViewportLayer viewportLayer;
	
	ColumnSpanHeaderDataProvider columnSpanHeaderDataProvider;
	SpanningDataLayer columnSpanHeaderDataLayer;
	SelectionLayer columnSpanHeaderSelectionLayer;
	ColumnHeaderLayer columnHeaderLayer;
	
	RowSpanHeaderDataProvider rowSpanHeaderDataProvider;
	SpanningDataLayer rowSpanHeaderDataLayer;
	RowHeaderLayer rowHeaderLayer;
	
	DefaultCornerDataProvider cornerDataProvider;
	DataLayer cornerDataLayer;
	CornerLayer cornerLayer;
	
	GridLayer gridLayer;
	
	NatTable natTable=null;
	
	public InstanceTable(Composite parent){
		super();
		this.parent = parent;
	}
	
	public InstanceTable(Composite parent,
			InstanceSelectionListener instanceSelectionListener) {
		super();
		this.parent = parent;
		this.instanceSelectionListener = instanceSelectionListener;
	}


	public NatTable getNatTable() {
		return natTable;
	}
	
	public InstanceSelectionListener getInstanceSelectionListener() {
		return instanceSelectionListener;
	}

	public void setInstanceSelectionListener(
			InstanceSelectionListener instanceSelectionListener) {
		this.instanceSelectionListener = instanceSelectionListener;
	}

	public void init(){
		rowCount=1;
		
		bodyDataProvider=new DodyDataProvider();
		bodyDataLayer=new DataLayer(bodyDataProvider, colWidth, rowHeight);
		selectionLayer = new SelectionLayer(bodyDataLayer);
		selectionLayer.addConfiguration(new DefaultSelectionLayerConfiguration());
		selectionLayer.addLayerListener(layerListener);
		viewportLayer = new ViewportLayer(selectionLayer);
		viewportLayer.setRegionName(GridRegion.BODY);
		
		columnSpanHeaderDataProvider=new ColumnSpanHeaderDataProvider();
		columnSpanHeaderDataLayer=new SpanningDataLayer(columnSpanHeaderDataProvider, colWidth, rowHeight);
//		columnHeaderLayer = new ColumnSpanHeaderLayer(columnSpanHeaderDataLayer, viewportLayer, selectionLayer, false);
		columnSpanHeaderSelectionLayer=new SelectionLayer(columnSpanHeaderDataLayer);
		columnHeaderLayer = new ColumnSpanHeaderLayer(columnSpanHeaderSelectionLayer, viewportLayer, selectionLayer, false);
		
		rowSpanHeaderDataProvider=new RowSpanHeaderDataProvider();
		rowSpanHeaderDataLayer=new SpanningDataLayer(rowSpanHeaderDataProvider, colWidth*2, rowHeight);
		rowHeaderLayer = new RowSpanHeaderLayer(rowSpanHeaderDataLayer, viewportLayer, selectionLayer, false);
		
		cornerDataProvider = new DefaultCornerDataProvider(columnSpanHeaderDataProvider, rowSpanHeaderDataProvider);
		cornerDataLayer = new DataLayer(cornerDataProvider, colWidth, rowHeight);
		cornerLayer = new CornerLayer(cornerDataLayer, rowHeaderLayer, columnHeaderLayer);
		
		gridLayer = new GridLayer(viewportLayer, columnHeaderLayer, rowHeaderLayer, cornerLayer);
	}
	
	public void update(ContainerListWrapper clw){
		lock.lock();
		this.clw = clw;
		sortedCntLists=clw.getSortedLists();
		for(List<StreamContainer> scList: sortedCntLists){
			int rowCountThisType=scList.size()/colSpanPerType;
			rowCountThisType += (scList.size()%colSpanPerType>0) ? 1 : 0;
			rowCount = Math.max(rowCount, rowCountThisType);
		}
		if(natTable==null){
			natTable = new NatTable(parent, gridLayer, false);      
	        natTable.addConfiguration(new DefaultNatTableStyleConfiguration());
	        natTable.addConfiguration(new HeaderMenuConfiguration(natTable));
	        this.natTable.addConfiguration(new PainterConfiguration());
	        natTable.configure();
	        natTable.setTheme(new ModernNatTableThemeConfiguration());
	        //ATT: parent should not set any layout
	        natTable.setBounds(0, 0, natTable.getPreferredWidth(), natTable.getPreferredHeight()+20);
		}
		refresh();
		//natTable.pack();
		lock.unlock();
	}
	
	public void doCommand(ILayerCommand command){
		if(natTable!=null){
			natTable.doCommand(command);
		}
	}
	
	public void refresh(){
		if(natTable!=null){
			natTable.refresh();
			natTable.redraw();
		}
	}
	
	public void notifyCellSelected(int columnIndex, int rowIndex){
		if(instanceSelectionListener!=null){
			lock.lock();
			StreamContainer sc=locateStreamContainer(columnIndex, rowIndex);
			if(sc!=null){
				instanceSelectionListener.instanceCellSelected(sc.getUniqueName(),this);
			}
			else{
				instanceSelectionListener.instanceCellSelected(null,this);
			}
        }
	}
	
	public StreamContainer locateStreamContainer(int columnIndex, int rowIndex) {
		int typeIndex=columnIndex/colSpanPerType;	
		int colIndexInType=columnIndex%colSpanPerType;
		int rowIndexInType=rowIndex;
		int indexInList= rowIndexInType*colSpanPerType + colIndexInType;
		List<StreamContainer> scList=sortedCntLists[typeIndex];
		
		if(indexInList<scList.size()){
			StreamContainer sc=scList.get(indexInList);
			return sc;
		}
		return null;
	}
	
	public Color getCellColor(int columnIndex, int rowIndex) {
		StreamContainer sc=locateStreamContainer(columnIndex, rowIndex);
		if(sc!=null){
			InstanceStat insStat=clw.getContainerStatMap().get(sc.getUniqueName());
			return InstanceColorFactory.getInstanceColor(insStat);
		}
		return GUIHelper.COLOR_WHITE;
	}

	class LayerListener implements ILayerListener{
		@Override
		public void handleLayerEvent(ILayerEvent event) {
			 if (event instanceof CellSelectionEvent) {
				 CellSelectionEvent cellEvent = (CellSelectionEvent) event;
				 if(cellEvent.getColumnPosition()<0 || cellEvent.getRowPosition()<0){
					 return;
				 }
                 System.out.format("cell (%d,%d)\n",
                		 cellEvent.getColumnPosition(),
                		 cellEvent.getRowPosition());
                 notifyCellSelected(cellEvent.getColumnPosition(),
                		 cellEvent.getRowPosition());
			 }
		}
	}
	
	class DodyDataProvider implements IDataProvider{
		@Override
		public Object getDataValue(int columnIndex, int rowIndex) {
			lock.lock();
			StreamContainer sc=locateStreamContainer(columnIndex, rowIndex);
			lock.unlock();
			String str="";
			if(sc!=null){
				InstanceStat insStat=clw.getContainerStatMap().get(sc.getUniqueName());
				str=sc.getUniqueName();
				str+="("+sc.getDownContainerIdList().size()+")\n";
				str+="("+NumberFormatter.format(insStat.getOutputRateSec())+")";
			}
			return str;
		}

		@Override
		public void setDataValue(int columnIndex, int rowIndex, Object newValue) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getColumnCount() {
			return colSpanPerType * typeHeaders.length;
		}

		@Override
		public int getRowCount() {
			return rowCount;
		}
	}
	
	class ColumnSpanHeaderDataProvider implements ISpanningDataProvider{
		@Override
		public void setDataValue(int columnIndex, int rowIndex, Object newValue) {
			throw new UnsupportedOperationException();
		}
		@Override
		public Object getDataValue(int columnIndex, int rowIndex) {
			int typeIndex=columnIndex/colSpanPerType;
			assert(typeIndex<typeHeaders.length);
			return typeHeaders[typeIndex];
		}
		@Override
		public int getColumnCount() {
			return colSpanPerType*typeHeaders.length;
		}
		@Override
		public int getRowCount() {
			return 1;
		}
		@Override
		public DataCell getCellByPosition(int columnPosition, int rowPosition) {
			int cellColumnPosition = columnPosition - columnPosition % colSpanPerType;
			int cellRowPosition = rowPosition;
			return new DataCell(cellColumnPosition, cellRowPosition, colSpanPerType, 1);
		}
	}
	
	class RowSpanHeaderDataProvider implements ISpanningDataProvider{
		@Override
		public void setDataValue(int columnIndex, int rowIndex, Object newValue) {
			throw new UnsupportedOperationException();
		}
		@Override
		public Object getDataValue(int columnIndex, int rowIndex) {
			return clw.getWorkerId().getId();
		}
		@Override
		public int getColumnCount() {
			return 1;
		}
		@Override
		public int getRowCount() {
			return rowCount;
		}
		@Override
		public DataCell getCellByPosition(int columnPosition, int rowPosition) {
			return new DataCell(0, 0, 1, getRowCount());
		}
	}
	
	class PainterConfiguration extends AbstractRegistryConfiguration {
		PainterConfiguration() {
		}

		@Override
		public void configureRegistry(IConfigRegistry configRegistry) {
			configRegistry.registerConfigAttribute(
					CellConfigAttributes.CELL_PAINTER, new ColorCellPainter(),
					"NORMAL", GridRegion.BODY);
		}
	}
	
	class ColorCellPainter extends TextPainter{
		@Override
	    public void paintCell(ILayerCell cell, GC gc, 
	    		Rectangle rectangle, IConfigRegistry configRegistry) {
			//System.out.format("rowIndex=%d, rowPosition=%d, colIndex=%d, colPosition=%d\n", 
					//cell.getRowIndex(), cell.getRowPosition(), 
					//cell.getColumnIndex(), cell.getColumnPosition());
			super.paintCell(cell, gc, rectangle, configRegistry);
		}
		
		@Override
		protected Color getBackgroundColour(ILayerCell cell, IConfigRegistry configRegistry) {
			int colIndex=cell.getColumnIndex();
			int rowIndex=cell.getRowIndex();
			
			return getCellColor(colIndex, rowIndex);
	    }
	}
}

class RowSpanHeaderLayer extends RowHeaderLayer{
	public RowSpanHeaderLayer(IUniqueIndexLayer baseLayer,
			ILayer verticalLayerDependency, SelectionLayer selectionLayer,
			boolean useDefaultConfiguration, ILayerPainter layerPainter) {
		super(baseLayer, verticalLayerDependency, selectionLayer,
				useDefaultConfiguration, layerPainter);
	}

	public RowSpanHeaderLayer(IUniqueIndexLayer baseLayer,
			ILayer verticalLayerDependency, SelectionLayer selectionLayer,
			boolean useDefaultConfiguration) {
		super(baseLayer, verticalLayerDependency, selectionLayer,
				useDefaultConfiguration);
	}

	public RowSpanHeaderLayer(IUniqueIndexLayer baseLayer,
			ILayer verticalLayerDependency, SelectionLayer selectionLayer) {
		super(baseLayer, verticalLayerDependency, selectionLayer);
	}
	
	@Override
    public ILayerCell getCellByPosition(int columnPosition, int rowPosition) {
		ILayerCell cell=this.getBaseLayer().getCellByPosition(columnPosition, rowPosition);
		return cell;
    }
}

class ColumnSpanHeaderLayer extends ColumnHeaderLayer{
	public ColumnSpanHeaderLayer(IUniqueIndexLayer baseLayer,
			ILayer horizontalLayerDependency,
			SelectionLayer selectionLayer, boolean useDefaultConfiguration,
			ILayerPainter layerPainter) {
		super(baseLayer, horizontalLayerDependency, selectionLayer,
				useDefaultConfiguration, layerPainter);
	}

	public ColumnSpanHeaderLayer(IUniqueIndexLayer baseLayer,
			ILayer horizontalLayerDependency,
			SelectionLayer selectionLayer, boolean useDefaultConfiguration) {
		super(baseLayer, horizontalLayerDependency, selectionLayer,
				useDefaultConfiguration);
	}

	public ColumnSpanHeaderLayer(IUniqueIndexLayer baseLayer,
			ILayer horizontalLayerDependency, SelectionLayer selectionLayer) {
		super(baseLayer, horizontalLayerDependency, selectionLayer);
	}
	@Override
    public ILayerCell getCellByPosition(int columnPosition, int rowPosition) {
		ILayerCell cell=this.getBaseLayer().getCellByPosition(columnPosition, rowPosition);
		return cell;
    }
}