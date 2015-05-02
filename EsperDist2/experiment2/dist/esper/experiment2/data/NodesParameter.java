package dist.esper.experiment2.data;

public class NodesParameter {
	public int	numWay;
	public int nodeCount;
	public int nodeCountPerType;
	public double equalRatio;
	public double implyRatio;
	
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
