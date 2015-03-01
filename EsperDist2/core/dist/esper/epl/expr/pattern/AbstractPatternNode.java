package dist.esper.epl.expr.pattern;

import java.io.*;
import java.util.*;

import com.espertech.esper.pattern.*;

import dist.esper.epl.expr.AbstractClause;
import dist.esper.epl.expr.AbstractExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.expr.util.Stringlizable;
import dist.esper.epl.sementic.IResolvable;

public abstract class AbstractPatternNode extends AbstractExpression{
	private static final long serialVersionUID = -1149420302150899330L;
	protected EvalFactoryNode factoryNode=null;
	public AbstractPatternNode parent=null;
	public AbstractPatternNode prev=null;
	public AbstractPatternNode next=null;
	public Map<EventAlias, List<Direction>> otherEventAliasDirectionListMap=new HashMap<EventAlias, List<Direction>>();
	
	//public abstract void computeIndepentSubPatterns();
	public abstract PatternPrecedenceEnum getPrecedence();
	public AbstractPatternNode getParent() {
		return parent;
	}
	public void setParent(AbstractPatternNode parent) {
		this.parent = parent;
	}
	public AbstractPatternNode getPrev() {
		return prev;
	}
	public void setPrev(AbstractPatternNode prev) {
		this.prev = prev;
	}
	public AbstractPatternNode getNext() {
		return next;
	}
	public void setNext(AbstractPatternNode next) {
		this.next = next;
	}
	
	public Map<EventAlias, List<Direction>> getEventAliasDirectinListMap() {
		return otherEventAliasDirectionListMap;
	}
	
	/**
	public abstract void dumpOwnEventAliases(Set<EventAlias> eaSet);
	public abstract void dumpOwnEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet);
	
	public Set<EventAlias> dumpOwnEventAliases(){
		Set<EventAlias> eaSet=new HashSet<EventAlias>();
		this.dumpAllEventAliases(eaSet);
		return eaSet;
	}
	
	public Set<EventOrPropertySpecification> dumpOwnEventOrPropertySpecReferences(){
		Set<EventOrPropertySpecification> epsSet=new HashSet<EventOrPropertySpecification>();
		this.dumpOwnEventOrPropertySpecReferences(epsSet);
		return epsSet;
	}
	*/
	
	public static class Direction{
		public static final Direction PREV=new Direction("prev",-1);
		public static final Direction PARENT=new Direction("parent",-2);
		public static final Direction CHILD=new Direction("child",-3);
		//public static final Direction CHILD_INDEX=new Direction(0);
		public static final Direction[] CHILD_IDNEXES;
		
		static final int MAX_CHILD_COUNT=32;
		
		String str;
		int value=Integer.MIN_VALUE;
		
		static{
			CHILD_IDNEXES=new Direction[MAX_CHILD_COUNT];
			for(int i=0; i<MAX_CHILD_COUNT; i++){
				CHILD_IDNEXES[i]=new Direction("child_index",i);
			}
		}
		
		private Direction(String str, int value) {
			this.str = str;
			this.value = value;
		}
		
		public static Direction indexedChild(int index){
			return CHILD_IDNEXES[index];
		}
		
		@Override
		public String toString(){
			if(value<0){
				return str;
			}
			else{
				return str+"["+value+"]";
			}
		}
	}
	
	public <T> T accept(IExpressionVisitor<T> visitor){
		return null;
	}
	
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return null;
	}
	
}
