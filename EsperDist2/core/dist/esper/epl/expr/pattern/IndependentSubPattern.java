package dist.esper.epl.expr.pattern;


import java.util.Arrays;

public class IndependentSubPattern {
	AbstractPatternNode[] nodes=null;

	public IndependentSubPattern(AbstractPatternNode[] nodes) {
		super();
		this.nodes = nodes;
	}

	public AbstractPatternNode[] getNodes() {
		return nodes;
	}

	public void setNodes(AbstractPatternNode[] nodes) {
		this.nodes = nodes;
	}
	
	@Override
	public String toString() {
		return Arrays.toString(nodes);
	}
}
