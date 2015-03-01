package dist.esper.epl.sementic;

@Deprecated
public interface IResolvable {
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception;
	public boolean resolveReference();
}
