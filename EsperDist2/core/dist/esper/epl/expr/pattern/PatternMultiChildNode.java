package dist.esper.epl.expr.pattern;


import java.lang.reflect.Array;
import java.util.*;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.pattern.AbstractPatternNode.Direction;
import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;

public abstract class PatternMultiChildNode extends AbstractPatternNode {
	private static final long serialVersionUID = 9045367366046475875L;
	ArrayList<AbstractPatternNode> childNodeList=new ArrayList<AbstractPatternNode>(4);
	ArrayList<IndependentSubPattern>[] idpSubPattenLists=null; 
	/*static final EigenCodeComparator comparator=new EigenCodeComparator();*/
	
	public PatternMultiChildNode(){
	}
	
	public void addChildNode(AbstractPatternNode pcn){
		childNodeList.add(pcn);
	}
	
	public List<AbstractPatternNode> getChildNodeList(){
		return childNodeList;
	}
	
	/**
	@Override
	public int eigenCode(){
		int code=this.getPrecedence().ordinal();
		for(PatternNode childNode: childNodeList){
			code ^= childNode.eigenCode();
		}
		return code;
	}
	*/
	
	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		for(AbstractPatternNode pcn: childNodeList){
			pcn.resolve(ssw, null);
		}
		
		if(this.getPrecedence()!=PatternPrecedenceEnum.FOLLOWEDBY){
			Collections.sort(childNodeList, null);//FIXME
		}
		AbstractPatternNode prev=null;
		for(AbstractPatternNode pcn: childNodeList){
			if(prev!=null){
				pcn.prev=prev;
				prev.next=pcn;
			}
			prev=pcn;
		}
		return true;
	}
	
	@Override
	public boolean resolveReference() {
		for(AbstractPatternNode pcn: childNodeList){
			pcn.resolveReference();
			Map<EventAlias, List<Direction>> childMap=pcn.getEventAliasDirectinListMap();
			for(Map.Entry<EventAlias, List<Direction>> entry: childMap.entrySet()){
				if(this.otherEventAliasDirectionListMap.containsKey(entry.getKey())){
					continue;
				}
				
				List<Direction> childPath=entry.getValue();
				int i;
				for(i=0; i<childPath.size(); i++){
					if(childPath.get(i)==Direction.PARENT){
						break;
					}
				}
				//assert(i==0);
				if(i<childPath.size()){
					List<Direction> path=childPath.subList(i+1, childPath.size());
					this.otherEventAliasDirectionListMap.put(entry.getKey(), path);
				}
			}
		}
		//System.out.println(this.toString()+"\n\t"+this.otherEventAliasDirectionListMap);
		return true;
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitPattenMultiChildNode(this);
	}
	
//	@SuppressWarnings("unchecked")
//	public void computeIndepentSubPatterns(){
//		for(PatternNode childNode: childNodeList){
//			childNode.computeIndepentSubPatterns();
//		}
//		
//		idpSubPattenLists=(ArrayList<IndependentSubPattern>[])
//				Array.newInstance(new ArrayList<IndependentSubPattern>(0).getClass() ,childNodeList.size());
//		for(int i=0; i<childNodeList.size(); i++){
//			ArrayList<IndependentSubPattern> ithList=new ArrayList<IndependentSubPattern>(childNodeList.size()-i);
//			for(int j=i; j<childNodeList.size(); j++){
//				PatternNode curNode=childNodeList.get(j);
//				boolean idpFlag=true;
//				for(Map.Entry<EventAlias, List<Direction>> entry: curNode.getEventAliasDirectinListMap().entrySet()){
//					List<Direction> path=entry.getValue();
//					if(path.contains(Direction.PARENT)){
//						idpFlag=false;
//						break;
//					}
//					int prevCount=0;
//					while(prevCount<path.size() && path.get(prevCount)==Direction.PREV){
//						prevCount++;
//					}
//					if(prevCount > j-i){
//						idpFlag=false;
//						break;
//					}
//				}
//				if(idpFlag){
//					PatternNode[] nodes=new PatternNode[j-i+1];
//					for(int k=i; k<=j; k++){
//						nodes[k-i]=childNodeList.get(k);
//					}
//					IndependentSubPattern idpSubPattern=new IndependentSubPattern(nodes);
//					ithList.add(idpSubPattern);
//				}
//				else{
//					break;
//				}
//			}
//			idpSubPattenLists[i]=ithList;
//		}
//		//print
//		System.out.println(this.toString());
//		for(int i=0; i<childNodeList.size(); i++){
//			System.out.println("\t"+childNodeList.get(i).toString() + " : "+this.idpSubPattenLists[i].toString());
//		}
//	}
	
	/**
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		for(PatternNode pcn: childNodeList){
			pcn.dumpAllEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpOwnEventAliases(Set<EventAlias> eaSet) {
		for(PatternNode pcn: childNodeList){
			pcn.dumpOwnEventAliases(eaSet);
		}
	}
	
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		for(PatternNode pcn: childNodeList){
			pcn.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	
	
	@Override
	public void dumpOwnEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		for(PatternNode pcn: childNodeList){
			pcn.dumpOwnEventOrPropertySpecReferences(epsSet);
		}
	}
	*/
	
	/**
	static class EigenCodeComparator implements Comparator<PatternNode>{
		@Override
		public int compare(PatternNode a, PatternNode b) {
			return Integer.bitCount(a.eigenCode()) - Integer.bitCount(b.eigenCode());
		}
	}
	*/
}
