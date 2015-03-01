package dist.esper.epl.expr;



import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class RowLimitClause extends AbstractClause{	
	public int numRows;
	public int optionalOffset;
	
	public RowLimitClause(int numRows, int optionalOffset) {
		super();
		this.numRows = numRows;
		this.optionalOffset = optionalOffset;
		this.type="limit";
	}

	public int getNumRows() {
		return numRows;
	}

	public void setNumRows(int numRows) {
		this.numRows = numRows;
	}

	public int getOptionalOffset() {
		return optionalOffset;
	}

	public void setOptionalOffset(int optionalOffset) {
		this.optionalOffset = optionalOffset;
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw){
		sw.append(type+" "+numRows);
		if(optionalOffset>0){
			sw.append("offset "+optionalOffset);
		}
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param)
			throws Exception {
		super.resolve(ssw, param);
		return true;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitRowLimitClause(this);
	}
}
