package dist.esper.test.util;

import java.util.*;

import dist.esper.util.StringUtil;

public class TestTreeBuildingAlgorithm {
	public static void main(String[] args){
		test1();
		//test2();
	}
	
	public static void test2(){
		int[] n={1, 2, 3, 4};
		int hash1=(((((n[0]<<1)^n[1])<<1)^n[2])<<1)^n[3];
		int hash2=(((n[0]<<1)^n[1])<<1) ^ ((n[0]<<1)^n[1]); 
		
		System.out.println(hash1);
		System.out.println(hash2);
	}
	
	public static void test1(){
		int length=5;
		Node[] n=new Node[length];
		for(int i=0;i<length;i++){
			n[i]=new Leaf(i);
		}
		TreeBuilder builder=new TreeBuilder();
		List<Node> rootList=builder.buildTree(n);
		
		System.out.println(rootList.size());
	}
}

class TreeBuilder{
	List<Node> rootList=new ArrayList<Node>();
	Map<Long,Node[]> map=new HashMap<Long,Node[]>();
	public List<Node> buildTree(Node[] leaves){
		rootList.clear();
		Arrays.sort(leaves, Node.comp);
		buildRecursively(leaves);
		return rootList;
	}
	
	public void buildRecursively(Node[] c){
		if(c.length==1){
			System.out.println(c[0].toString());
			rootList.add(c[0]);
			return;
		}		
		for(int i=0;i<c.length;i++){
			for(int j=i+1;j<c.length;j++){
				if(c.length==4 && i==0 && j==2){
					System.out.print("");
				}
				if(c.length==3 && i==1 && j==2){
					System.out.print("");
				}
				if(Math.abs(c[i].getLevel()-c[j].getLevel())<=1){
					Node[] c2=buildNewNodeArray(c, i, j);
					Arrays.sort(c2, Node.comp);
					
					//int hashCode=hashCode(c, i, j);
					int hashCode=hashCode(c2);
					if(!map.containsKey(Long.valueOf(hashCode))){
						map.put((long)hashCode, c2);
						buildRecursively(c2);
					}
					else{
						System.out.print("");
						int hashCode2=hashCode(c2);
					}
				}
			}
		}
	}
	
	public Node[] buildNewNodeArray(Node[] c, int i, int j){
		Branch b=new Branch(c[i], c[j]);
		Node[] c2=new Node[c.length-1];
		c2[0]=b;
		int t=1;
		for(int k=0;k<c.length;k++){
			if(k!=i && k!=j){
				c2[t]=c[k];
				t++;
			}
		}
		assert(t==c2.length);
		return c2;
	}
	
//	public int hashCode(Node[] c, int i1, int i2){
//		int hashCode=0;
//		for(int i=0;i<c.length;i++){
//			if(i!=i1 && i!=i2){
//				hashCode ^= hashCode(c[i]); 
//			}
//		}
//		int branchHashCode=getBranchHashCode(c[i1], c[i2]);//the same as Branch Node
//		hashCode ^= branchHashCode;
//		return hashCode;
//	}
	
	public int hashCode(Node[] c){
		int hashCode=0;
		for(int i=0;i<c.length;i++){
			//hashCode = (hashCode>>1) ^ hashCode(c[i]);
			hashCode = hashCode(c[i], hashCode);
		}
		
		return hashCode;
	}
	
	public int hashCode(Node n, int h){
		if(n instanceof Leaf){
			return h*31 + n.hashCode(); 
		}
		else if(n instanceof Branch){
			Branch b=(Branch)n;
			h = h*31 + 501;
			for(Node child: b.childList){
				//h = h*31 + child.hashCode();
				//h = h*31 + hashCode(child);
				h = hashCode(child, h);
			}
			h = h*31 + 997;
			//return getBranchHashCode(b.getChild(0), b.getChild(1));
		}
		return h;
	}
	
	public int hashCode(Node n){
//		if(n instanceof Leaf){
//			return n.hashCode(); 
//		}
//		else if(n instanceof Branch){
//			Branch b=(Branch)n;
//			return getBranchHashCode(b.getChild(0), b.getChild(1));
//		}
//		return 0;
		return hashCode(n, 0);
	}
	
//	public int getBranchHashCode(Node c1, Node c2){
//		int a=503;
//		int b=901;
//		int c=701;
//		int d=1601;
//		int hash1=hashCode(c1);
//		int hash2=hashCode(c2);
//		return (((((a*31+hash1)*31)+b)*31)+hash2)*c+d;
////		if(c1.getLevel()>c2.getLevel() || hash1<hash2){
////			return (hash1<<1) ^ hash2;
////		}
////		else{
////			return (hash2<<1) ^ hash1;
////		}
//	}
}

class NodeComparator implements Comparator<Node>{

	@Override
	public int compare(Node n1, Node n2) {
		int l1=n1.getLevel();
		int l2=n2.getLevel();
		if(l1!=l2){
			return l1-l2;
		}
		else{
			return n1.getMin()-n2.getMin();
		}
	}	
}

abstract class Node{
	static NodeComparator comp=new NodeComparator(); 
	public Node() {
		super();		
	}	
	public abstract int getLevel();
	public abstract void toStringBuilder(StringBuilder sb, int indent);
	public abstract int getMin();
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		this.toStringBuilder(sb, 0);
		return sb.toString();
	}
}

class Leaf extends Node{
	int value;
	public Leaf(int value) {
		this.value = value;	
	}

	@Override
	public int getLevel() {
		return 1;
	}
	
	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}
	
	@Override
	public String toString(){
		return "Leaf[value="+value+"]";
	}

	@Override
	public void toStringBuilder(StringBuilder sb, int indent) {
		sb.append(StringUtil.getSpaces(indent));
		sb.append(this.toString());
		sb.append('\n');
	}

	@Override
	public int getMin() {
		return value;
	}
}

class Branch extends Node{
	List<Node> childList=new ArrayList<Node>(2);
	public Branch(Node child1, Node child2) {
		childList.add(child1);
		childList.add(child2);
		Collections.sort(childList, comp);
	}
	
	public Node getChild(int index){
		return childList.get(index);
	}

	@Override
	public int getLevel() {
		int childMaxLevel=0;
		for(Node child: childList){
			childMaxLevel=(child.getLevel()>childMaxLevel)?child.getLevel():childMaxLevel;
		}
		return childMaxLevel+1;
	}

	@Override
	public void toStringBuilder(StringBuilder sb, int indent) {
		sb.append(StringUtil.getSpaces(indent));
		sb.append("Branch");
		sb.append('\n');
		for(Node child: childList){
			child.toStringBuilder(sb, indent+4);
		}
	}

	@Override
	public int getMin() {
		int min=Integer.MAX_VALUE;
		for(Node child: childList){
			int childMin=child.getMin();
			min=(childMin<min)?childMin:min;
		}
		return min;
	}
}

//class Root extends Node{
//	Node child;
//	public Root() {
//		super();
//	}
//	public Node getChild() {
//		return child;
//	}
//	public void setChild(Node child) {
//		this.child = child;
//	}
//	@Override
//	public int getLevel() {
//		return child.getLevel()+1;
//	}	
//}