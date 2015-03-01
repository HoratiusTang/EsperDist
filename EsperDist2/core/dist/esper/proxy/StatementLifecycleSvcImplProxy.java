package dist.esper.proxy;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import com.espertech.esper.client.EPStatementException;
import com.espertech.esper.client.annotation.Hint;
import com.espertech.esper.client.annotation.Name;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import com.espertech.esper.core.service.*;
import com.espertech.esper.core.service.StatementLifecycleSvcImpl.EPStatementDesc;
import com.espertech.esper.core.start.EPStatementStartMethod;
import com.espertech.esper.epl.annotation.AnnotationUtil;
import com.espertech.esper.epl.expression.ExprNodeSubselectDeclaredDotVisitor;
import com.espertech.esper.epl.expression.ExprSubselectNode;
import com.espertech.esper.epl.expression.ExprValidationException;
import com.espertech.esper.epl.spec.*;
import com.espertech.esper.epl.spec.util.StatementSpecRawAnalyzer;
import com.espertech.esper.util.UuidGenerator;

public class StatementLifecycleSvcImplProxy {
	public StatementLifecycleSvcImpl slv=null;
	
	public StatementLifecycleSvcImplProxy(StatementLifecycleSvc slv){
		super();
		this.slv=(StatementLifecycleSvcImpl)slv;
	}
	
	public StatementSpecCompiled createAndStart(StatementSpecRaw statementSpec, String expression, boolean isPattern, String optStatementName, Object userObject, EPIsolationUnitServices isolationUnitServices, String statementId, EPStatementObjectModel optionalModel){
		String assignedStatementId = statementId;
        if (assignedStatementId == null) {
            assignedStatementId = UuidGenerator.generate();
        }
        return createStoppedAssignName(statementSpec, expression, isPattern, optStatementName, assignedStatementId, null, userObject, isolationUnitServices, optionalModel);
	}
	
	public StatementSpecCompiled createStoppedAssignName(StatementSpecRaw statementSpec, String expression, boolean isPattern, String optStatementName, String statementId, Map<String, Object> optAdditionalContext, Object userObject, EPIsolationUnitServices isolationUnitServices, EPStatementObjectModel optionalModel){
		boolean nameProvided = false;
        String statementName = statementId;

        // compile annotations, can produce a null array
        Annotation[] annotations = AnnotationUtil.compileAnnotations(statementSpec.getAnnotations(), slv.services.getEngineImportService(), expression);

        // find name annotation
        if (optStatementName == null) {
            if (annotations != null && annotations.length != 0) {
                for (Annotation annotation : annotations) {
                    if (annotation instanceof Name) {
                        Name name = (Name) annotation;
                        if (name.value() != null) {
                            optStatementName = name.value();
                        }
                    }
                }
            }
        }

        // Determine a statement name, i.e. use the id or use/generate one for the name passed in
        if (optStatementName != null) {
            optStatementName = optStatementName.trim();
            statementName = slv.getUniqueStatementName(optStatementName, statementId);
            nameProvided = true;
        }

        return createStopped(statementSpec, annotations, expression, isPattern, statementName, nameProvided, statementId, optAdditionalContext, userObject, isolationUnitServices, false, optionalModel);
	}
	
	protected synchronized StatementSpecCompiled createStopped(StatementSpecRaw statementSpec,
            Annotation[] annotations,
            String expression,
            boolean isPattern,
            String statementName,
            boolean nameProvided,
            String statementId,
            Map<String, Object> optAdditionalContext,
            Object userObject,
            EPIsolationUnitServices isolationUnitServices,
            boolean isFailed,
            EPStatementObjectModel optionalModel){
		EPStatementDesc statementDesc;
        EPStatementStartMethod startMethod;

        // Hint annotations are often driven by variables
        if (annotations != null)
        {
            for (Annotation annotation : annotations)
            {
                if (annotation instanceof Hint)
                {
                    statementSpec.setHasVariables(true);
                }
            }
        }

        // walk subselects, declared expressions, dot-expressions
        ExprNodeSubselectDeclaredDotVisitor visitor;
        try {
            visitor = StatementSpecRawAnalyzer.walkSubselectAndDeclaredDotExpr(statementSpec);
        }
        catch (ExprValidationException ex) {
            throw new EPStatementException(ex.getMessage(), expression);
        }

        // Determine Subselects for compilation, and lambda-expression shortcut syntax for named windows
        List<ExprSubselectNode> subselectNodes = visitor.getSubselects();
        if (!visitor.getChainedExpressionsDot().isEmpty()) {
        	StatementLifecycleSvcImpl.rewriteNamedWindowSubselect(visitor.getChainedExpressionsDot(), subselectNodes, slv.services.getNamedWindowService());
        }

        // compile foreign scripts
        slv.validateScripts(expression, statementSpec.getScriptExpressions(), statementSpec.getExpressionDeclDesc());

        // Determine stateless statement
        boolean stateless = StatementLifecycleSvcImpl.determineStatelessSelect(statementSpec, !subselectNodes.isEmpty(), isPattern);
        

        // Make context
        StatementContext statementContext = slv.services.getStatementContextFactory().makeContext(statementId,  statementName, expression, slv.services, optAdditionalContext, false, annotations, isolationUnitServices, stateless, statementSpec);

        StatementSpecCompiled compiledSpec;
        try
        {
            compiledSpec = StatementLifecycleSvcImpl.compile(statementSpec, expression, statementContext, false, false, annotations, visitor.getSubselects(), visitor.getDeclaredExpressions(), slv.services);
            return compiledSpec;
        }
        catch (EPStatementException ex)
        {
            slv.stmtNameToIdMap.remove(statementName); // Clean out the statement name as it's already assigned
            throw ex;
        }

	}
}
