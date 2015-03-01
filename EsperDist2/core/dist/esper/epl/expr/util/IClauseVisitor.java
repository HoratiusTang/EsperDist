package dist.esper.epl.expr.util;

import dist.esper.epl.expr.*;

public interface IClauseVisitor<T> {
	public T visitBooleanExpressionClause(BooleanExpressionClause bec);
	public T visitFromClause(FromClause fc);
	public T visitGroupByClause(GroupByClause gbc);
	public T visitOrderByClause(OrderByClause obc);
	public T visitOrderByElement(OrderByElement obe);
	public T visitRowLimitClause(RowLimitClause rlc);
	public T visitSelectClause(SelectClause sc);
	public T visitSelectClauseExpressionElement(SelectClauseExpressionElement sce);
	public T visitSelectClauseWildcardElement(SelectClauseWildcardElement sce);
	public T visitStatementSpecification(StatementSpecification ss);
	public T visitFilterStreamSpecification(FilterStreamSpecification fss);
	public T visitPatternStreamSpecification(PatternStreamSpecification pss);
}
