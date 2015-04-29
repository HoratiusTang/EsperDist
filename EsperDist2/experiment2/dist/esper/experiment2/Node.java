package dist.esper.experiment2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {
	public static NodeComparator nodeComp=new NodeComparator();
	//public NodeType type;
	public int id;
	public int tag;
	public int numWay;
	public int row;
	public int col;
	public Set<Node> equalNodeSet=new TreeSet<Node>(nodeComp);
	public Set<Node> implyNodeSet=new TreeSet<Node>(nodeComp);	
	static AtomicInteger UID=new AtomicInteger(0);
	
	public Node() {
		super();		
		this.id = UID.getAndIncrement();
		this.tag = this.id;
	}
	
	public int getTag() {
		return tag;
	}
	
	public void setTag(int tag) {
		this.tag = tag;
	}
	
	public int getRow() {
		return row;
	}

	public int getNumWay() {
		return numWay;
	}

	public void setNumWay(int numWay) {
		this.numWay = numWay;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getColumn() {
		return col;
	}

	public void setColumn(int col) {
		this.col = col;
	}
	
	public void addEqualNode(Node n){
		equalNodeSet.add(n);
	}
	
	public void addImplyNode(Node n){
		implyNodeSet.add(n);
	}
	//	public static enum NodeType{
//		NONE("none"),
//		FITLER("filter"),
//		JOIN("join");
//		private String string;
//		private NodeType(String str){
//			this.string = str;
//		}
//		@Override
//		public String toString(){
//			return string;
//		}
//	}
	public static class NodeComparator implements Comparator<Node>{
		@Override
		public int compare(Node n1, Node n2) {
			return n2.col - n1.col; //reverse
		}
	}
}
