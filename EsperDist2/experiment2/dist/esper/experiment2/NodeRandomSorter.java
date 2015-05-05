package dist.esper.experiment2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import dist.esper.experiment2.data.Node;

public class NodeRandomSorter {
	Random rand=new Random();
	double implyRatio=0.50;
	double deltaImplyRatio=0.05;
	int[] bc; //bigger count
	int[] sc; //smaller count
	NodeComparator nodeComparator=new NodeComparator();
	
	public void randomSort(Node[] n, double implyRatio, double deltaImplyRatio)
		throws Exception{
		if(n==null || n.length<=1){
			return;
		}
		this.implyRatio = implyRatio;
		this.deltaImplyRatio = deltaImplyRatio;
		bc=new int[n.length];
		sc=new int[n.length];
		
		int swapCount=n.length/4;
		if(this.implyRatio<0.1 && n.length>5){
			nodeComparator.setAscOrder(false);
			Arrays.sort(n, nodeComparator);
			swapCount = n.length/10 + 1;
//			if(!check(n)){
//				System.err.format("warning: in NodeRandomSorter.randomSort() sorted nodes in desc orders, but still check failed.");
//				System.err.flush();
//			}
//			return;
		}
		else if(this.implyRatio>0.6){
			nodeComparator.setAscOrder(true);
			Arrays.sort(n, nodeComparator);
			swapCount = n.length/10 + 1;
		}
		int totalSwapCount=0;
		while(true){
			if(check(n)){
				return;
			}
			if(totalSwapCount>=n.length*n.length){
				throw new Exception(String.format("can't sort %d nodes according to arguments: implyRatio=%.2f, deltaImplyRatio=%.2f", 
						n.length, this.implyRatio, this.deltaImplyRatio));
			}
			randomSortOnce(n, swapCount);
			totalSwapCount+=swapCount;
		}
	}
	
	public void randomSortOnce(Node[] n, int swapCount){
		Node temp;
		int j,k;
		for(int i=0; i<swapCount; i++){
			j=rand.nextInt(n.length);
			k=rand.nextInt(n.length);
			if(j!=k){
				temp=n[j];
				n[j]=n[k];
				n[k]=temp;
			}
		}
	}
	
	public boolean check(Node[] n){
		for(int i=0; i<n.length; i++){
			bc[i]=0;
			for(int j=0; j<i; j++){
				if(n[i].getTag() > n[j].getTag()){
					bc[i]++;
				}
			}
		}
		double tbc=0.0;
		for(int i=1; i<n.length; i++){
			tbc += bc[i]/(double)i;
		}
		tbc = tbc/(n.length-1);
		
		if(tbc>implyRatio-deltaImplyRatio && 
			tbc<implyRatio+deltaImplyRatio){
			return true;
		}
		return false;
	}
	
	class NodeComparator implements Comparator<Node>{
		boolean asc=true;
		@Override
		public int compare(Node n1, Node n2) {
			if(asc)
				return n1.getTag()-n2.getTag();
			else
				return n2.getTag()-n1.getTag();
		}
		public void setAscOrder(boolean asc){
			this.asc=asc;
		}
	}
}
