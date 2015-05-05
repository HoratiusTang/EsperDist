package dist.esper.experiment2.data;

public class NodesParameter {
	public int	numWay;
	public int nodeCount=0;
	public int nodeCountPerType=0;
	public double equalRatio=0;
	public double implyRatio=0;
	
	public NodesParameter(int numWay){
		this.numWay = numWay;
	}
	
	public NodesParameter(int numWay, int nodeCount, int nodeCountPerType, 
			double equalRatio, double implyRatio) {
		super();
		this.numWay = numWay;
		this.nodeCount = nodeCount;
		this.nodeCountPerType = nodeCountPerType;
		this.equalRatio = equalRatio;
		this.implyRatio = implyRatio;
	}
	
	@Override
	public String toString(){
		return String.format("%d-%d-%d-%.2f-%.2f",numWay,nodeCount,nodeCountPerType,equalRatio,implyRatio);
	}
}
