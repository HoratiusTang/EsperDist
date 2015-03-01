package dist.esper.epl.expr;


import java.util.HashSet;
import java.util.Set;

import com.espertech.esper.epl.spec.PatternStreamSpecCompiled;

import dist.esper.epl.expr.pattern.AbstractPatternNode;
import dist.esper.epl.expr.pattern.PatternNodeFactory;
import dist.esper.epl.expr.util.EventAliasDumper;
import dist.esper.epl.expr.util.IClauseVisitor;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class PatternStreamSpecification extends StreamSpecification {
	private PatternStreamSpecCompiled patternStreamSpec=null;
	public AbstractPatternNode patternNode=null;
	
	public PatternStreamSpecification(PatternStreamSpecCompiled patternStreamSpec) {
		super();
		this.patternStreamSpec = patternStreamSpec;
	}
	
	public static class Factory{
		public static PatternStreamSpecification make(PatternStreamSpecCompiled pssc){
			PatternStreamSpecification pss=new PatternStreamSpecification(pssc);
			pss.setOptionalStreamName(pss.patternStreamSpec.getOptionalStreamName());
			ViewSpecification[] vss=ViewSpecification.Factory.makeViewSpecs(pss.patternStreamSpec.getViewSpecs());
			pss.setViewSpecs(vss);
			pss.patternNode=PatternNodeFactory.makeNode(pss.patternStreamSpec.getEvalFactoryNode(), null);
			return pss;
		}
	}

	public AbstractPatternNode getPatternNode() {
		return patternNode;
	}

	public void setPatternNode(AbstractPatternNode patternNode) {
		this.patternNode = patternNode;
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append("pattern[");
		patternNode.toStringBuilder(sw);
		sw.append("]");
		if(this.viewSpecs!=null && this.viewSpecs.length>0){
			for(ViewSpecification vs: viewSpecs){
				sw.append(".");
				vs.toStringBuilder(sw);
			}
		}
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) throws Exception{
		patternNode.resolve(ssw, null);
		patternNode.resolveReference();
		//patternNode.computeIndepentSubPatterns();
		
		//test
		/**Set<EventAlias> allSet=new HashSet<EventAlias>();
		patternNode.dumpAllEventAliases(allSet);*/
		Set<EventAlias> allSet=EventAliasDumper.dump(patternNode);
		
		/**Set<EventAlias> ownSet=new HashSet<EventAlias>();
		patternNode.dumpOwnEventAliases(ownSet);*/
		Set<EventAlias> ownSet=EventAliasDumper.dump(patternNode);//FIXME: different
		
		System.out.println(allSet);
		System.out.println(ownSet);
		return true;
	}
	
	@Override
	public <T> T accept(IClauseVisitor<T> visitor){
		return visitor.visitPatternStreamSpecification(this);
	}
}
