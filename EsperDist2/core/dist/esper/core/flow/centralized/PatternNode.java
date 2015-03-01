package dist.esper.core.flow.centralized;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.pattern.AbstractPatternNode;
import dist.esper.util.StringUtil;

/**
 * the node presents a pattern stream in 'FROM' clause,
 * e.g. 'pattern [every [2](a=A-> [2]b=B(b[0].age>a.age))].win:time(5 second)'
 * 
 * @author tjy
 *
 */
public class PatternNode extends BaseNode {
	AbstractPatternNode patternNode=null;
	public PatternNode(AbstractPatternNode patternNode) {
		super();
		this.patternNode = patternNode;
		this.eplId = patternNode.getEplId();
	}
	
	public PatternNode(PatternNode psn){
		this.patternNode=psn.patternNode;
		this.viewSpecs=psn.viewSpecs;
		this.optionalStreamName=psn.optionalStreamName;
		this.eplId = psn.getEplId();
	}

	public AbstractPatternNode getPatternNode() {
		return patternNode;
	}

	public void setPatternNode(AbstractPatternNode patternNode) {
		this.patternNode = patternNode;
	}

	@Override
	public void dumpSelectedEventAliases(Set<EventAlias> eaSet) {
		/*patternNode.dumpOwnEventAliases(eaSet);*/
		throw new RuntimeException("not implemented yet");
	}

	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		NodeStringlizer.toStringBuilder(this, sw, indent);
	}
}
