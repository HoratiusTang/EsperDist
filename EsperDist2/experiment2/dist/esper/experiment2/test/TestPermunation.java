package dist.esper.experiment2.test;

import java.util.Random;

public class TestPermunation {
	static int length=20;
	static int k=3;
	static double dk=0.2d;
	static int dk2=2;
	static int[] b=new int[length];
	static int[] s=new int[length];
	static Integer[] a=new Integer[length];
	static StringBuilder sb=new StringBuilder();
	public static void main(String[] args){
		test1();
	}
	
	public static void test1(){
		for(int i=0;i<a.length;i++){
			a[i]=Integer.valueOf(i);
		}
		
		int k, j;
		Integer temp;
		Random r=new Random();
		
		for(int i=0;i<1000;i++){
			k=r.nextInt(length);
			j=r.nextInt(length);
			if(k!=j){
				temp=a[k];
				a[k]=a[j];
				a[j]=temp;
				check();
			}
		}
		System.out.println("finished");
	}
	
	public static void check(){
		for(int i=0;i<a.length;i++){
			b[i]=0;
			for(int j=0;j<i;j++){
				if(a[i].intValue() > a[j].intValue()){
					b[i]++;
				}
			}
//			if(i>=k && b[i]<=k-dk2){
//				return;
//			}
		}
		
		double tb=0; 
		for(int i=0;i<a.length;i++){
			tb+=b[i];
		}
		tb=tb/a.length;
		//----------------------------------------
		for(int i=0;i<a.length;i++){
			s[i]=0;
			for(int j=i+1;j<a.length;j++){
				if(a[i].intValue() < a[j].intValue()){
					s[i]++;
				}
			}
//			if(i<a.length-k && s[i]>k+dk2){
//				return;
//			}
		}
		
		double ts=0; 
		for(int i=0;i<a.length;i++){
			ts+=s[i];
		}
		ts=ts/a.length;
		//-----------------------------------------		
		
		if(tb>=k-dk && tb<=k+dk && ts>=k-dk && ts<=k+dk){
			sb.setLength(0);
			for(int i=0;i<a.length;i++){
				sb.append(String.format("%d[%d][%d] ",a[i],b[i],s[i]));
			}
			sb.append(String.format("(%.2f, %.2f)", tb, ts));
			System.out.println(sb.toString());
		}
	}
}
