package dist.esper.epl.expr;


import java.util.Set;

import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.Stringlizable;
import dist.esper.epl.sementic.IResolvable;
import dist.esper.epl.sementic.StatementSementicWrapper;

public abstract class AbstractClause  implements Stringlizable, IResolvable{
	long eplId;
	public String type="";
	
	public long getEplId() {
		return eplId;
	}

	public void setEplId(long eplId) {
		this.eplId = eplId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	public boolean resolveReference(){
		return true;
	}
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		this.eplId=ssw.getEplId();
		return true;
	}
	
	public <T> T accept(IClauseVisitor<T> visitor){
		return null;
	}
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw);
		return sw.toString();
	}
}
