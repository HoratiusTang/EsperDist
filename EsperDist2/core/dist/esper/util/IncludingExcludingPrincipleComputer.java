package dist.esper.util;

import java.util.*;

public class IncludingExcludingPrincipleComputer {
	
	public static double compute(double[] vals){
		List<DataWrapper> dataList=new LinkedList<DataWrapper>();
		for(int i=0;i<vals.length;i++){
			DataWrapper data=new DataWrapper(vals.length, i, vals[i]);
			dataList.add(data);
		}
	
		double result=sum(dataList);
		for(int i=2; i<=vals.length; i++){
			dataList=mul(dataList, vals);
			double sum=sum(dataList);
			result += (i%2==0)?(-sum):sum;
		}
		return result;
	}
	
	public static List<DataWrapper> mul(List<DataWrapper> dataList, double[] vals){
		List<DataWrapper> newDataList=new LinkedList<DataWrapper>();
		for(DataWrapper data: dataList){
			for(int i=data.getMaxIndex()+1; i<vals.length; i++){
				DataWrapper newData=data.mul(i, vals[i]);
				newDataList.add(newData);
			}
		}
		return newDataList;
	}
	
	public static double sum(List<DataWrapper> dataList){
		double sum=0.0;
		for(DataWrapper data: dataList){
			sum += data.product;
		}
		return sum;
	}
}

class DataWrapper{	
	int maxIndex;
	double product=1.0d;	
	
	public DataWrapper() {
	}
	
	public DataWrapper(int size, int initIndex, double initVal){
		maxIndex=initIndex;		
		product=initVal;
	}
	
	public DataWrapper mul(int index, double val){
		DataWrapper result=new DataWrapper();		
		result.setMaxIndex(Math.max(this.maxIndex, index));
		result.setProduct(this.product* val);
		return result;
	}

	public int getMaxIndex() {
		return maxIndex;
	}

	public void setMaxIndex(int maxIndex) {
		this.maxIndex = maxIndex;
	}

	public double getProduct() {
		return product;
	}

	public void setProduct(double product) {
		this.product = product;
	}
}
