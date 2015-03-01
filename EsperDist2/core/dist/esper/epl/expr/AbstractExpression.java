package dist.esper.epl.expr;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import dist.esper.epl.expr.util.ExpressionStringlizer;
import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.expr.util.Stringlizable;
import dist.esper.epl.sementic.IResolvable;
import dist.esper.epl.sementic.StatementSementicWrapper;

public abstract class AbstractExpression implements Serializable, Stringlizable, IResolvable{	
	private static final long serialVersionUID = 5534527556580283046L;
	long eplId;
	
	public AbstractExpression() {
		super();
	}

	/**
	public int eigenCode(){
		return hashCode();
	}
	*/
	
	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}
	
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		this.eplId=ssw.getEplId();
		return true;
	}

	public abstract <T> T accept(IExpressionVisitor<T> visitor);
	
	public abstract <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj);
	
	@Override
	public void toStringBuilder(StringBuilder sb){
		ExpressionStringlizer.getInstance().toStringBuilder(this, sb);
	}
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw);
		return sw.toString();
	}
	
	/**
	public abstract void dumpAllEventAliases(Set<EventAlias> eaSet);
	public abstract void dumpAllEventOrPropertySpecReferences(Set<EventOrPropertySpecification> epsSet);
	
	public void dumpOwnEventAliases(Set<EventAlias> eaSet){
		this.dumpAllEventAliases(eaSet);
	}
	
	public Set<EventAlias> dumpOwnEventAliases(){
		Set<EventAlias> eaSet=new HashSet<EventAlias>();
		this.dumpAllEventAliases(eaSet);
		return eaSet;
	}
	
	public Set<EventAlias> dumpAllEventAliases(){
		Set<EventAlias> eaSet=new HashSet<EventAlias>();
		this.dumpAllEventAliases(eaSet);
		return eaSet;
	}
	
	public Set<EventOrPropertySpecification> dumpAllEventOrPropertySpecReferences(){
		Set<EventOrPropertySpecification> epsSet=new HashSet<EventOrPropertySpecification>();
		this.dumpAllEventOrPropertySpecReferences(epsSet);
		return epsSet;
	}
	*/
}
