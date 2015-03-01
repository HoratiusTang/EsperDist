package dist.esper.monitor.ui.custom;

import java.util.*;

import org.eclipse.swt.widgets.Composite;
import org.eclipse.zest.core.widgets.Graph;

public class CGraph extends Graph {
	Map<Object, Object> map=new TreeMap<Object, Object>();
	Object data;
	public CGraph(Composite parent, int style) {
		super(parent, style);
	}
	
	@Override
	public void setData(String key, Object value){
		map.put(key, value);
	}
	
	@Override
	public Object getData(String key){
		return map.get(key);
	}
	
	@Override
	public void setData(Object data){
		this.data = data;
	}
	
	@Override
	public Object getData(){
		return data;
	}
	
	public void removeData(){
		data=null;
	}
	
	public void removeData(String key){
		map.remove(key);
	}
	
	public void removeMappedData(){
		map.clear();
	}
	
	public void removeAllData(){
		removeData();
		removeMappedData();
	}
}
