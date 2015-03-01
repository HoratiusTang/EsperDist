package dist.esper.epl.expr.pattern;


import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.pattern.AbstractPatternNode.Direction;
import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;

public abstract class PatternSingleChildNode extends AbstractPatternNode{
	private static final long serialVersionUID = -1200664188159411789L;
	public AbstractPatternNode childNode=null;
	public boolean isChildIndependent=false;
	
	public PatternSingleChildNode(){
	}
	
	public PatternSingleChildNode(AbstractPatternNode childNode){
		this.childNode=childNode;
	}
	
	public void setChildNode(AbstractPatternNode childNode){
		this.childNode=childNode;
	}
	
	public AbstractPatternNode getChildNode(){
		return childNode;
	}
	
	/**
	@Override
	public int eigenCode(){
		int code=this.getPrecedence().ordinal();
		code ^= childNode.eigenCode();
		return code;
	}
	*/
	
	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		return childNode.resolve(ssw,null);
	}
	
	@Override
	public boolean resolveReference() {
		childNode.resolveReference();
		Map<EventAlias, List<Direction>> childMap=childNode.getEventAliasDirectinListMap();
		for(Map.Entry<EventAlias, List<Direction>> entry: childMap.entrySet()){
			List<Direction> childPath=entry.getValue();
			int i;
			for(i=0; i<childPath.size(); i++){
				if(childPath.get(i)==Direction.PARENT){
					break;
				}
			}
			assert(i==0);
			if(i<childPath.size()){
				List<Direction> path=childPath.subList(i+1, childPath.size());
				this.otherEventAliasDirectionListMap.put(entry.getKey(), path);
			}
		}
		//System.out.println(this.toString()+"\n\t"+this.otherEventAliasDirectionListMap);
		return true;
	}
	
//	public void computeIndepentSubPatterns(){
//		childNode.computeIndepentSubPatterns();
//		isChildIndependent=true;
//		for(Map.Entry<EventAlias, List<Direction>> entry: childNode.getEventAliasDirectinListMap().entrySet()){
//			List<Direction> path=entry.getValue();
//			if(path.contains(Direction.PARENT)){
//				isChildIndependent=false;
//				break;
//			}
//		}
//		System.out.println(this.toString());
//		System.out.println("\t"+childNode.toString() + " : isChildIndependent="+this.isChildIndependent);
//	}
	
	/**
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet){
		childNode.dumpAllEventOrPropertySpecReferences(epsSet);
	}
	
	@Override
	public void dumpOwnEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet){
		childNode.dumpOwnEventOrPropertySpecReferences(epsSet);
	}

	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		childNode.dumpAllEventAliases(eaSet);
	}
	
	@Override
	public void dumpOwnEventAliases(Set<EventAlias> eaSet) {
		childNode.dumpOwnEventAliases(eaSet);
	}
	*/
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitPattenSingleChildNode(this);
	}
}
