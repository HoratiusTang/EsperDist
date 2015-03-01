package dist.esper.core.flow.centralized;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import dist.esper.epl.expr.AbstractResultExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.SelectClauseExpressionElement;

/**
 * the abstract node class in @Tree,
 * it's the super-class of all other nodes.
 * @author tjy
 *
 */
public abstract class Node {
	static AtomicLong UID=new AtomicLong(0L);
	long id;
	long eplId;
	
	/** the outputs send to upper-level node */
	List<SelectClauseExpressionElement> resultElementList=new ArrayList<SelectClauseExpressionElement>(4);
	
	public Node(){
		id=UID.getAndIncrement();
	}
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public void setResultElementList(
			List<SelectClauseExpressionElement> resultElementList) {
		this.resultElementList = resultElementList;
	}
	public List<SelectClauseExpressionElement> getResultElementList(){
		return resultElementList;
	}
	public void addResultElement(SelectClauseExpressionElement resultExpr){
		this.resultElementList.add(resultExpr);
	}
	public long getEplId() {
		return eplId;
	}
	public void setEplId(long eplId) {
		this.eplId = eplId;
	}

	public abstract int getLevel();
	public abstract void dumpSelectedEventAliases(Set<EventAlias> eaSet);
	
	public Set<EventAlias> dumpSelectedEventAliases(){
		Set<EventAlias> eaSet=new HashSet<EventAlias>();
		this.dumpSelectedEventAliases(eaSet);
		return eaSet;
	}
	
	public abstract void toStringBuilder(StringBuilder sw, int indent);
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw, 0);
		return sw.toString();
	}
}
