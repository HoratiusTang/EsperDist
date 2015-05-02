package dist.esper.experiment2;

import java.util.Random;

import dist.esper.experiment2.data.Node;

public class NodeRandomSorter {
	Random rand=new Random();
	double implyRatio=3;
	double deltaImplyRatio=0.2d;
	int[] bc; //bigger count
	int[] sc; //smaller count
	
	public void randomSort(Node[] n, double implyRatio, double deltaImplyRatio){
		if(n==null || n.length<=1){
			return;
		}
		this.implyRatio = implyRatio;
		this.deltaImplyRatio = deltaImplyRatio;
		bc=new int[n.length];
		sc=new int[n.length];
		
		while(true){
			randomSortOnce(n);
			if(check(n)){
				return;
			}
		}
	}
	
	public void randomSortOnce(Node[] n){
		Node temp;
		int j,k;
		for(int i=0; i<n.length; i++){
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
}
