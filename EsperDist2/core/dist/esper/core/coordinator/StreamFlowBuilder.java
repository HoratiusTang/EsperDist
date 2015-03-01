package dist.esper.core.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dist.esper.core.*;
import dist.esper.core.flow.centralized.*;
import dist.esper.core.flow.stream.*;
import dist.esper.epl.expr.EventAlias;

public class StreamFlowBuilder {
	Coordinator coordinator;
	
	public StreamFlowBuilder(Coordinator coordinator) {
		super();
		this.coordinator = coordinator;
	}

	/**
	public List<StreamFlow> buildStreamFlow(Tree tree){
		List<StreamFlow> stList=new ArrayList<StreamFlow>();
		RootStream root=(RootStream)buildStreamRecursively(tree.getRoot());
		
		StreamFlow st=new StreamFlow(tree.getEplId(), tree.getEpl() , root);
		stList.add(st);
		return stList;
	}
	*/
	
	public StreamFlow buildStreamFlow(Tree tree){
		RootStream root=(RootStream)buildStreamRecursively(tree.getRoot());
		StreamFlow sf=new StreamFlow(tree.getEplId(), tree.getEpl() , root);
		return sf;
	}
	
	public Stream buildStreamRecursively(Node sn){
		Stream sl=null;
		if(sn instanceof RootNode){
			RootNode rn=(RootNode)sn;
			DerivedStream child=(DerivedStream)buildStreamRecursively(rn.getChild());
			RootStream rsl=new RootStream(child);
			rsl.setWindowTimeUS(child.getWindowTimeUS());
			rsl.setWhereExprList(rn.getWhereExprList());
			rsl.setResultElementList(sn.getResultElementList());
			sl=rsl;
		}
		else if(sn instanceof JoinNode){
			JoinNode jn=(JoinNode)sn;
			JoinStream jsl=new JoinStream();
			long maxWindowTimeUS=0;
			for(Node c: jn.getChildList()){
				DerivedStream child=(DerivedStream)buildStreamRecursively(c);
				maxWindowTimeUS=(maxWindowTimeUS>child.getWindowTimeUS())?maxWindowTimeUS:child.getWindowTimeUS();
				jsl.addUpStream(child);
			}
			jsl.setWindowTimeUS(maxWindowTimeUS);
			jsl.setJoinExprList(jn.getJoinExprList());
			jsl.setResultElementList(sn.getResultElementList());
			sl=jsl;
		}
		else if(sn instanceof FilterNode){
			FilterNode fsn=(FilterNode)sn;
			FilterStream fsl=new FilterStream(fsn.getEventSpec(), fsn.getFilterExpr());
			fsl.setOptionalStreamName(fsn.getOptionalStreamName());
			fsl.setViewSpecs(fsn.getViewSpecs());
			fsl.setResultElementList(sn.getResultElementList());
			Set<EventAlias> eaSet=fsn.dumpSelectedEventAliases();
			assert(eaSet.size()>0);
			EventAlias eventAlias=null;
			for(EventAlias ea: eaSet){
				eventAlias=ea;
				break;
			}
			RawStream rsl=getRawStreamByEventAlias(eventAlias);
			fsl.setRawStream(rsl);
			sl=fsl;
		}
		else if(sn instanceof PatternNode){
			PatternNode psn=(PatternNode)sn;
			PatternStream psl=new PatternStream(psn.getPatternNode());
			psl.setOptionalStreamName(psn.getOptionalStreamName());
			psl.setViewSpecs(psn.getViewSpecs());
			psl.setResultElementList(sn.getResultElementList());
			Set<EventAlias> eaSet=psn.dumpSelectedEventAliases();
			for(EventAlias ea: eaSet){
				RawStream rsl=getRawStreamByEventAlias(ea);
				psl.addRawStream(rsl);
			}
			sl=psl;
		}
		sl.setEplId(sn.getEplId());
		return sl;
	}
	
	public RawStream getRawStreamByEventAlias(EventAlias ea){
		for(RawStream rsl: coordinator.getRawStreamList()){
			if(ea.getEvent().equals(rsl.getInternalCompositeEvent())){					
				return rsl;
			}
		}
		return null;
	}
}
