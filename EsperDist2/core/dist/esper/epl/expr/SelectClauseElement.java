package dist.esper.epl.expr;

import java.io.Serializable;


import dist.esper.epl.sementic.IResolvable;
import dist.esper.epl.sementic.StatementSementicWrapper;

public abstract class SelectClauseElement  extends AbstractClause implements Serializable{
	private static final long serialVersionUID = 4628914290926064757L;

	public abstract void toStringBuilder(StringBuilder sw);
	
	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		super.resolve(ssw, param);
		return true;
	}
}
