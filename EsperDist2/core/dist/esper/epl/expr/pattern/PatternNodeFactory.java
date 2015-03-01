package dist.esper.epl.expr.pattern;

import java.io.*;
import java.util.*;

import com.espertech.esper.filter.FilterSpecCompiled;
import com.espertech.esper.pattern.*;

public class PatternNodeFactory {
	public static AbstractPatternNode makeNode(EvalFactoryNode factoryNode, AbstractPatternNode parent){
		if(factoryNode instanceof EvalAndFactoryNode){
			EvalAndFactoryNode fn=(EvalAndFactoryNode)factoryNode;
			List<EvalFactoryNode> fcns=fn.getChildNodes();
			PatternAndNode pn=new PatternAndNode(fn, parent);
			for(EvalFactoryNode fcn: fcns){
				AbstractPatternNode pcn=makeNode(fcn, pn);
				pn.addChildNode(pcn);
			}
			return pn;
		}
		
		else if(factoryNode instanceof EvalEveryFactoryNode){
			EvalEveryFactoryNode fn=(EvalEveryFactoryNode)factoryNode;
			List<EvalFactoryNode> fcns=fn.getChildNodes();
			assert(fcns.size()<=1):String.format("List<EvalFactoryNode>.size()=%d",fcns.size());			
			PatternEveryNode pn=new PatternEveryNode(fn, parent);
			for(EvalFactoryNode fcn: fcns){
				AbstractPatternNode pcn=makeNode(fcn, pn);
				pn.setChildNode(pcn);//FIXME
			}
			return pn;
		}
		else if(factoryNode instanceof EvalFilterFactoryNode){
			EvalFilterFactoryNode fn=(EvalFilterFactoryNode)factoryNode;			
			PatternFilterNode pn=PatternFilterNode.Factory.make(fn, parent);
			return pn;
		}
		else if(factoryNode instanceof EvalFollowedByFactoryNode){
			EvalFollowedByFactoryNode fn=(EvalFollowedByFactoryNode)factoryNode;
			List<EvalFactoryNode> fcns=fn.getChildNodes();
			PatternFollowedByNode pn=new PatternFollowedByNode(fn, parent);
			for(EvalFactoryNode fcn: fcns){
				AbstractPatternNode pcn=makeNode(fcn, pn);
				pn.addChildNode(pcn);
			}
			return pn;
		}
		else if(factoryNode instanceof EvalGuardFactoryNode){
			EvalGuardFactoryNode fn=(EvalGuardFactoryNode)factoryNode;
			List<EvalFactoryNode> fcns=fn.getChildNodes();
			PatternGuardNode pn=PatternGuardNode.Factory.make(fn, parent);
			//pn.setPatternGuardSpec(fn.getPatternGuardSpec());
			//for(EvalFactoryNode fcn: fcns){
				AbstractPatternNode pcn=makeNode(fcns.get(0), pn);
				pn.setChildNode(pcn);
			//}
			return pn;
		}
		else if(factoryNode instanceof EvalMatchUntilFactoryNode){
			EvalMatchUntilFactoryNode fn=(EvalMatchUntilFactoryNode)factoryNode;
			List<EvalFactoryNode> fcns=fn.getChildNodes();
			assert(fcns.size()<=1):String.format("List<EvalFactoryNode>.size()=%d",fcns.size());
			PatternMatchUntilNode pn=PatternMatchUntilNode.Factory.make(fn, parent);
			for(EvalFactoryNode fcn: fcns){
				AbstractPatternNode pcn=makeNode(fcn, pn);
				pn.setChildNode(pcn);//FIXME
			}
			return pn;
		}
		else if(factoryNode instanceof EvalObserverFactoryNode){
			EvalObserverFactoryNode fn=(EvalObserverFactoryNode)factoryNode;
			List<EvalFactoryNode> fcns=fn.getChildNodes();
			PatternObserverNode pn=PatternObserverNode.Factory.make(fn, parent);
			//pn.setPatternObserverSpec(fn.getPatternObserverSpec());
			for(EvalFactoryNode fcn: fcns){
				AbstractPatternNode pcn=makeNode(fcn, pn);
				pn.setChildNode(pcn);
			}
			return pn;
		}
		else if(factoryNode instanceof EvalOrFactoryNode){
			EvalOrFactoryNode fn=(EvalOrFactoryNode)factoryNode;
			List<EvalFactoryNode> fcns=fn.getChildNodes();
			PatternOrNode pn=new PatternOrNode(fn, parent);
			for(EvalFactoryNode fcn: fcns){
				AbstractPatternNode pcn=makeNode(fcn, pn);
				pn.addChildNode(pcn);
			}
			return pn;
		}
		return null;
	}
	
	public static void toEPL(StringBuilder sw, List<AbstractPatternNode> childNodes, PatternPrecedenceEnum precedence){
		childNodes.get(0).toStringBuilder(sw);
		for(int i=1;i<childNodes.size();i++){
			sw.append(" ");
			sw.append(precedence.getString());
			sw.append(" ");
			childNodes.get(i).toStringBuilder(sw);
		}
	}
}
