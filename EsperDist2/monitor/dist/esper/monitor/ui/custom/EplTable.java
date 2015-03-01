package dist.esper.monitor.ui.custom;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.*;

public class EplTable{
	Table table;
	public String[] tableColumnNames=new String[]{
			"id",
			"SELECT",
			"FROM",
			"WHERE",
	};
	
	//public static final String EPL_ID="eplId";
	
	public Table getTable() {
		return table;
	}

	public EplTable(Composite parent, int style) {
		table=new Table(parent, style);
		table.setHeaderVisible(true);
		for(int i=0;i<tableColumnNames.length;i++){
			TableColumn col=new TableColumn(table, SWT.NONE);
			col.setText(tableColumnNames[i]);
		}
	}
	
	public TableItem addItem(long eplId, String epl){
		epl=epl.trim();
		TableItem item=new TableItem(table, SWT.NONE);
		item.setText(0, ""+eplId);
		item.setText(1, epl);
		
		String epl1=epl.toUpperCase();
		int selectIndex=epl1.indexOf(tableColumnNames[1]);
		int fromIndex=epl1.indexOf(tableColumnNames[2], selectIndex+tableColumnNames[1].length());
		int whereIndex=epl1.indexOf(tableColumnNames[3], fromIndex+tableColumnNames[2].length());
		whereIndex=(whereIndex>0)?whereIndex:epl1.length();
		
		item.setText(1, epl.substring(selectIndex,fromIndex));
		item.setText(2, epl.substring(fromIndex, whereIndex));
		item.setText(3, epl.substring(whereIndex, epl.length()));
		
		//item.setBackground(table.getDisplay().getSystemColor(SWT.COLOR_GRAY));
		//item.setData(EPL_ID, Long.valueOf(eplId));
		return item;
	}
	
	public void removeAll(){
		table.removeAll();
	}
	
	public void pack(){
		for(TableColumn col: table.getColumns()){
			col.pack();
		}
		table.pack();
	}
	
}
