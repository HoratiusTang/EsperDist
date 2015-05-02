package dist.esper.experiment2.data;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {
	int id;
	int tag;
	int numWay;
	static AtomicInteger UID=new AtomicInteger(0);
	
	List<SelectElement> selectElementList=new ArrayList<SelectElement>(10);
	
	public Node() {
		super();		
		this.id = UID.getAndIncrement();
		this.tag = this.id;
	}	
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public int getTag() {
		return tag;
	}
	
	public void setTag(int tag) {
		this.tag = tag;
	}
	
	public int getNumWay() {
		return numWay;
	}

	public void setNumWay(int numWay) {
		this.numWay = numWay;
	}
	
	public void addSelectElement(SelectElement se){
		selectElementList.add(se);
	}
	
	public List<SelectElement> getSelectElementList() {
		return selectElementList;
	}
	
	public void setSelectElementList(List<SelectElement> selectElementList) {
		this.selectElementList = selectElementList;
	}
}
