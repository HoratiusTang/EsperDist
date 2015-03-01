package dist.esper.epl.expr.pattern;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.espertech.esper.filter.*;
import com.espertech.esper.pattern.*;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.EventAliasDumper;
import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.event.Event;

public class PatternFilterNode extends PatternNoChildNode{
	private static final long serialVersionUID = -2796686711226596464L;
	//EventAlias eventAlias=null;
	EventSpecification eventSpec=null;
	AbstractBooleanExpression filterExpr=null;
	String eventAsName;
	String eventTypeName;
	
	public PatternFilterNode(){
	}
	
	public PatternFilterNode(EvalFilterFactoryNode factoryNode, AbstractPatternNode parent){
		this.factoryNode=factoryNode;
		this.parent=parent;
		this.eventAsName=factoryNode.getEventAsName();
		this.eventTypeName=factoryNode.getFilterSpec().getFilterForEventTypeName();
	}	
	
	public EventSpecification getEventSpec() {
		return eventSpec;
	}

	public void setEventSpec(EventSpecification eventSpec) {
		this.eventSpec = eventSpec;
	}

	public EventAlias getEventAlias() {
		return eventSpec.getEventAlias();
	}

//	public void setEventAlias(EventAlias eventAlias) {
//		this.eventAlias = eventAlias;
//	}	
	
	public String getEventAsName(){
		return eventAsName;
	}
	
	public String getEventTypeName(){
		return eventTypeName;
	}
	
	public AbstractBooleanExpression getFilterExpression(){
		return filterExpr;
	}
	
	public void setFilterExpression(AbstractBooleanExpression filterExpr){
		this.filterExpr=filterExpr;
	}
	
	public static class Factory{
		public static PatternFilterNode make(EvalFilterFactoryNode factoryNode, AbstractPatternNode parent){
			PatternFilterNode pfn=new PatternFilterNode(factoryNode, parent);
			FilterSpecCompiled fsc=((EvalFilterFactoryNode)factoryNode).getFilterSpec();
			FilterSpecParam[] fsps=fsc.getParameters();
			if(fsps!=null && fsps.length>0){
//				if(fsps[0] instanceof FilterSpecParamExprNode){
//					FilterSpecParamExprNode fspen=(FilterSpecParamExprNode)fsps[0];
//					pfn.setFilterExpression((IBooleanExpression)(ExpressionFactory.toExpression(fspen.getExprNode())));
//				}
				pfn.filterExpr=FilterFactory.toBooleanExpression(fsps, pfn.getEventAsName());
			}
			return pfn;
		}
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append("(");
		if (getEventAsName() != null) {
			sw.append(getEventAsName());
			sw.append("=");
		}
		sw.append(getEventTypeName());
		if(filterExpr!=null){
			//sw.append("(");
			filterExpr.toStringBuilder(sw);
			//sw.append(")");
		}
		sw.append(")");
	}

	@Override
	public PatternPrecedenceEnum getPrecedence() {
		return PatternPrecedenceEnum.FILTER;
	}
	
	/**
	@Override
	public int eigenCode(){
		int code = this.getPrecedence().ordinal();
		code ^= eventSpec.getEventAlias().eigenCode();
		if(filterExpr!=null){
			code ^= filterExpr.eigenCode();
		}
		return code;
	}
	*/
	
	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		Event event=ssw.eventRegistry.resolveEvent(getEventTypeName());
		assert(event!=null):getEventTypeName();
		EventAlias eventAlias=new EventAlias(ssw.eplId, event,this);
		eventSpec=new EventSpecification(eventAlias);
		
		AbstractPatternNode p=this.parent;
		while(p!=null){
			if(p instanceof PatternMatchUntilNode){
				eventSpec.setArray(true);
				break;
			}
			p=p.parent;
		}
		
		if(getEventAsName()!=null){
			eventAlias.setEventAsName(getEventAsName());
			ssw.eventAliasMap.put(getEventAsName(), eventAlias);
		}
		if(filterExpr!=null){
			filterExpr.resolve(ssw,eventAlias);
		}
		return true;//FIXME: to register EventAlias
	}

	@Override
	public boolean resolveReference() {
		Set<EventAlias> eaSet=new HashSet<EventAlias>();
		/*dumpAllEventAliases(eaSet);*/
		EventAliasDumper.dump(this, eaSet);
		eaSet.remove(eventSpec.getEventAlias());
		
		EventAliasPatternNodeLocator eaLocator=new EventAliasPatternNodeLocator();
		for(EventAlias ea: eaSet){
			List<Direction> path=eaLocator.locate(ea);
			this.otherEventAliasDirectionListMap.put(ea, path);
			//System.out.println(path);
		}
		//System.out.println(this.toString()+"\n\t"+this.otherEventAliasDirectionListMap);
		return true;
	}
	
	public void computeIndepentSubPatterns(){
		
	}
	
	/**
	@Override
	public void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		epsSet.add(this.eventSpec);
		if(filterExpr!=null){
			filterExpr.dumpAllEventOrPropertySpecReferences(epsSet);
		}
	}
	
	
	@Override
	public void dumpOwnEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet) {
		epsSet.add(this.eventSpec);
	}
	
	
	@Override
	public void dumpAllEventAliases(Set<EventAlias> eaSet) {
		eaSet.add(eventSpec.getEventAlias());
		if(filterExpr!=null){
			filterExpr.dumpAllEventAliases(eaSet);
		}
	}
	
	
	@Override
	public void dumpOwnEventAliases(Set<EventAlias> eaSet) {
		eaSet.add(eventSpec.getEventAlias());
	}
	*/
	
	class EventAliasPatternNodeLocator{
		public List<Direction> locate(EventAlias ea){
			Stack<Direction> path=new Stack<Direction>();
			IdentityHashMap<AbstractPatternNode,Object> map=new IdentityHashMap<AbstractPatternNode,Object>();
			boolean result=recursivelyLocatePatternNode(ea, PatternFilterNode.this, path, map);
			if(result){
				List<Direction> pathList=new ArrayList<Direction>();
				pathList.addAll(path);
				return pathList;
			}
			return null;
		}
		
		protected boolean recursiveDown(EventAlias ea, AbstractPatternNode curNode, Stack<Direction> path, IdentityHashMap<AbstractPatternNode,Object> map){
			boolean result=false;
			
			if(map.containsKey(curNode)){
				return result;
			}
			map.put(curNode, null);
			
			if(curNode instanceof PatternNoChildNode) {
				if(curNode instanceof PatternNoChildNode) {
					PatternFilterNode filterNode=(PatternFilterNode)curNode;
					if(filterNode.getEventAlias()==ea){//or filterNode == ea.getSource
						result=true;
					}
				}
			}
			
			if(!result && curNode instanceof PatternSingleChildNode){
				PatternSingleChildNode scNode=(PatternSingleChildNode)curNode;
				path.push(Direction.CHILD);
				result=recursiveDown(ea, scNode.getChildNode(), path, map);
				if(!result){
					path.pop();
				}
			}
			if(!result && curNode instanceof PatternMultiChildNode){
				PatternMultiChildNode mcNode=(PatternMultiChildNode)curNode;
				for(int i=0;i<mcNode.getChildNodeList().size();i++){
					path.push(Direction.indexedChild(i));
					result=recursiveDown(ea, mcNode.getChildNodeList().get(i), path, map);
					if(!result){
						path.pop();
					}
					else{
						return result;
					}
				}
			}
			return result;
		}
		
		protected boolean recursivelyLocatePatternNode(EventAlias ea, AbstractPatternNode curNode, Stack<Direction> path, IdentityHashMap<AbstractPatternNode,Object> map){
			boolean result=false;
			result=recursiveDown(ea, curNode, path, map);
			
			AbstractPatternNode prevNode=curNode.getPrev();
			while(!result && prevNode!=null){
				path.push(Direction.PREV);
				result=recursiveDown(ea, prevNode, path, map);
				if(!result){
					path.pop();
					prevNode=prevNode.getPrev();
				}
			}
				
			if(!result && curNode.getParent()!=null){
				path.push(Direction.PARENT);
				result=recursivelyLocatePatternNode(ea, curNode.getParent(), path, map);
				if(!result){
					path.pop();
				}
			}
			return result;
		}
	}
	
	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitPattenFilterNode(this);
	}
}
