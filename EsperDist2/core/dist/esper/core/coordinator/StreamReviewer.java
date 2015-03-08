package dist.esper.core.coordinator;

import java.util.*;

import dist.esper.core.flow.container.*;
import dist.esper.core.flow.container.DerivedStreamContainer.StreamAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.*;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResultOfAgent;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.util.*;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.*;
import dist.esper.util.CollectionUtils;
import dist.esper.util.Logger2;
import dist.esper.util.Tuple2D;

public class StreamReviewer {
	static Logger2 log=Logger2.getLogger(StreamReviewer.class);
	List<FilterStreamContainer> existedFscList;
	List<PatternStreamContainer> existedPscList;
	List<JoinStreamContainer> existedJscList;
	List<RootStreamContainer> existedRscList;
	Map<StreamAndContainer,BooleanExpressionComparisonResult> crMap=new HashMap<StreamAndContainer,BooleanExpressionComparisonResult>();
	RootStream rootStream;
	ExpressionComparator exprComp=new ExpressionComparator();
	BooleanExpressionComparator beComp=new BooleanExpressionComparator(exprComp);

	public StreamReviewer(
			List<FilterStreamContainer> existedFslcList,
			List<PatternStreamContainer> existedPslcList,
			List<JoinStreamContainer> existedJslcList,
			List<RootStreamContainer> existedRslcList){
		this(existedFslcList, existedPslcList, existedJslcList, existedRslcList, null);
	}
	
	public StreamReviewer(
			List<FilterStreamContainer> existedFslcList,
			List<PatternStreamContainer> existedPslcList,
			List<JoinStreamContainer> existedJslcList,
			List<RootStreamContainer> existedRslcList,
			RootStream rootStream) {
		super();
		this.existedFscList = existedFslcList;
		this.existedPscList = existedPslcList;
		this.existedJscList = existedJslcList;
		this.existedRscList = existedRslcList;
		this.rootStream = rootStream;
	}
	
	public BooleanExpressionComparisonResult getBoolComparisonResult(DerivedStream sl,
		DerivedStreamContainer sc){
		StreamAndContainer lc=new StreamAndContainer(sl,sc);
		return crMap.get(lc);
	}
	
	public void addBoolComparisonResult(DerivedStream sl,
			DerivedStreamContainer sc,
			BooleanExpressionComparisonResult cr){
		StreamAndContainer lc=new StreamAndContainer(sl,sc);
		crMap.put(lc,cr);
	}
	
	public BooleanExpressionComparisonResult check(Stream sl, StreamContainer sc, long parentContainerEplId,
			Map<EventAlias, EventAlias> parentContainerMap){
		if((sl instanceof FilterStream) && (sc instanceof FilterDelayedStreamContainer)){
			return checkFslAndFcslc((FilterStream)sl, (FilterDelayedStreamContainer)sc, parentContainerEplId, parentContainerMap);
		}
		else if((sl instanceof FilterStream) && (sc instanceof FilterStreamContainer)){
			return checkFslAndFslc((FilterStream)sl, (FilterStreamContainer)sc, parentContainerEplId, parentContainerMap);
		}
		else if((sl instanceof JoinStream) && (sc instanceof JoinDelayedStreamContainer)){
			return checkJslAndJcslc((JoinStream)sl, (JoinDelayedStreamContainer)sc, parentContainerEplId, parentContainerMap);
		}
		else if((sl instanceof JoinStream) && (sc instanceof JoinStreamContainer)){
			return checkJslAndJslc((JoinStream)sl, (JoinStreamContainer)sc, parentContainerEplId, parentContainerMap);
		}
		else if((sl instanceof PatternStream) && (sc instanceof PatternStreamContainer)){
			return checkPslAndPslc((PatternStream)sl, (PatternStreamContainer)sc, parentContainerEplId, parentContainerMap);
		}
		else if((sl instanceof RootStream) && (sc instanceof RootStreamContainer)){
			return checkRslAndRslc((RootStream)sl, (RootStreamContainer)sc, parentContainerEplId, parentContainerMap);
		}
		return null;//indicate can't be direct or indirect reused
	}
	
	//fsl.eplId=4, fsc.eplId=2, parent.eplId=1
	public BooleanExpressionComparisonResult checkFslAndFslc(FilterStream fsl, FilterStreamContainer fsc, long parentContainerEplId,
			Map<EventAlias, EventAlias> parentContainerMap){
		BooleanExpressionComparisonResult cr=this.getBoolComparisonResult(fsl, fsc);
		if(cr!=null){
			if(cr.getTotalState()==State.NONE){
				return cr;
			}
			
			else if(cr.getTotalState()==State.EQUIVALENT || cr.getTotalState()==State.IMPLYING){//already added
				StreamAndMapAndBoolComparisonResult parentLmcr=
						(StreamAndMapAndBoolComparisonResult)fsc.getDirectOrIndirectReuseStreamMapComparisonResultByEplId(parentContainerEplId);
				//parentMap: 1->4
				parentContainerMap.put(((FilterStream)parentLmcr.getFirst()).getEventSpec().getEventAlias(), 
						fsl.getEventSpec().getEventAlias());
				return cr;
			}
		}
		else{//cr=null
			if(fsl.getEventSpec().getEventAlias().getEvent()==fsc.getEventSpec().getEventAlias().getEvent()){
				Map<EventAlias, EventAlias> fscToFslMap=new HashMap<EventAlias, EventAlias>(1);
				fscToFslMap.put(fsc.getEventSpec().getEventAlias(), fsl.getEventSpec().getEventAlias());
				exprComp.setEventAliasMap(fscToFslMap);				
				cr=beComp.compare(fsl.getFilterExpr(), fsc.getFilterExpr());//FIXME: fsc is compatibleContainer
				if(cr!=null && (cr.getOwnState()==State.EQUIVALENT || cr.getOwnState()==State.IMPLYING)){
					if(cr.getOwnState()==State.EQUIVALENT){
						fsl.addDirectReusableContainerMapComparisonResult(fsc, fscToFslMap, cr);
					}
					else{
						fsl.addIndirectReusableContainerMapComparisonResult(fsc, fscToFslMap, cr);
					}
					StreamAndMapAndBoolComparisonResult parentLmcr=
							(StreamAndMapAndBoolComparisonResult)fsc.getDirectOrIndirectReuseStreamMapComparisonResultByEplId(parentContainerEplId);
					//parentMap: 1->4
					parentContainerMap.put(((FilterStream)parentLmcr.getFirst()).getEventSpec().getEventAlias(), 
							fsl.getEventSpec().getEventAlias());
//					FilterStreamLocation parentEplfsl=(FilterStreamLocation)fsc.getStreamLocationByEplId(parentContainerEplId);
//					parentContainerMap.put(parentEplfsl.getEventSpec().getEventAlias(), fsl.getEventSpec().getEventAlias());
				}
			}
		}
		if(cr==null){
			cr=BooleanExpressionComparisonResultNone.getInstance();
		}
		addBoolComparisonResult(fsl, fsc, cr);
		return cr;
	}
	
	private static List<AbstractBooleanExpression> dumpImplyingAndSurplusConditionList(List<BooleanExpressionComparisonPair> cpList){
		List<AbstractBooleanExpression> condList=new ArrayList<AbstractBooleanExpression>(cpList.size());
		for(BooleanExpressionComparisonPair cp: cpList){
			if(cp.getState()==State.SURPLUS || cp.getState()==State.IMPLYING){
				condList.add(cp.getFirst());
			}
		}
		return condList;
	}
	public BooleanExpressionComparisonResult checkFslAndFcslc(FilterStream fsl, FilterDelayedStreamContainer fcsc, long parentContainerEplId,
			Map<EventAlias, EventAlias> parentContainerMap){
		BooleanExpressionComparisonResult cr=this.getBoolComparisonResult(fsl, fcsc);
		if(cr!=null){
			if(cr.getTotalState()==State.NONE){
				return cr;
			}
			else{
				StreamAndMapAndBoolComparisonResult lm=fcsc.getDirectOrIndirectReuseStreamMapComparisonResultByEplId(parentContainerEplId);//location->container, the location is container in parent
				ContainerAndMapAndBoolComparisonResult cm=null;
				if(cr.getTotalState()==State.EQUIVALENT){
					cm=fsl.getDirectReusableContainerMapBoolComparisonResultById(fcsc.getEplId());//container->location
				}
				else if(cr.getTotalState()==State.IMPLYING){
					//parent.eplId=0, jsc.eplId=1, jsl.eplId=4
					cm=fsl.getIndirectReusableContainerMapBoolComparisonResultById(fcsc.getEplId());//container->location
				}
				CollectionUtils.merge(lm.getSecond(), //0->1
						cm.getSecond(), //1->4
						parentContainerMap); //0->4
				return cr;
			}
		}
		else if(fsl.getBaseStreamCount()==fcsc.getBaseStreamCount() && fsl.getRawStreamCount()==fcsc.getRawStreamCount() &&
				!(fcsc.getAgent() instanceof FilterDelayedStreamContainer)){
			BooleanExpressionComparisonResult agentCr=null;
			Map<EventAlias, EventAlias> fcscEplMap=new HashMap<EventAlias, EventAlias>(4);
			agentCr=check(fsl, fcsc.getAgent(), fcsc.getEplId(), fcscEplMap);
			if(agentCr!=null && agentCr.getTotalState()==State.IMPLYING){
				exprComp.setEventAliasMap(fcscEplMap);
				List<AbstractBooleanExpression> fslExtraFilterCondList=dumpImplyingAndSurplusConditionList(agentCr.getOwnPairList());
				cr=beComp.compareConjunctionLists(fslExtraFilterCondList, fcsc.getExtraFilterCondList(), 0);
				if(cr.getTotalState()==State.EQUIVALENT || cr.getTotalState()==State.IMPLYING){
					Map<EventAlias, EventAlias> fcscAgentEplMap=new HashMap<EventAlias, EventAlias>(4);
					agentCr=check(fsl, fcsc.getAgent(), fcsc.getAgent().getEplId(), fcscAgentEplMap);
					
					ContainerAndMapAndBoolComparisonResult cmcr=new ContainerAndMapAndBoolComparisonResult(fcsc, fcscEplMap, cr);
					ContainerAndMapAndBoolComparisonResult agentCmcr=new ContainerAndMapAndBoolComparisonResult(fcsc.getAgent(), fcscAgentEplMap, agentCr);
					ContainerAndMapAndBoolComparisonResultOfAgent cmcr2=new ContainerAndMapAndBoolComparisonResultOfAgent(cmcr, agentCmcr);
					
					if(cr.getTotalState()==State.EQUIVALENT){
						fsl.addDirectReusableContainerMapComparisonResult(cmcr2);
					}
					else{
						fsl.addIndirectReusableContainerMapComparisonResult(cmcr2);
					}				
					check(fsl, fcsc.getAgent(), parentContainerEplId, parentContainerMap);
				}
			}
		}
		
		if(cr==null){
			cr=BooleanExpressionComparisonResultNone.getInstance();
		}
		addBoolComparisonResult(fsl, fcsc, cr);
		return cr;
	}
	
	public BooleanExpressionComparisonResult checkJslAndJcslc(JoinStream jsl, JoinDelayedStreamContainer jcsc, long parentContainerEplId,
			Map<EventAlias, EventAlias> parentContainerMap){
		BooleanExpressionComparisonResult cr=this.getBoolComparisonResult(jsl, jcsc);
		
		if(cr!=null){
			if(cr.getTotalState()==State.NONE){
				return cr;
			}
			else{
				StreamAndMapAndBoolComparisonResult lm=jcsc.getDirectOrIndirectReuseStreamMapComparisonResultByEplId(parentContainerEplId);//location->container, the location is container in parent
				ContainerAndMapAndBoolComparisonResult cm=null;
				if(cr.getTotalState()==State.EQUIVALENT){
					cm=jsl.getDirectReusableContainerMapBoolComparisonResultById(jcsc.getEplId());//container->location
				}
				else if(cr.getTotalState()==State.IMPLYING){
					//parent.eplId=0, jsc.eplId=1, jsl.eplId=4
					cm=jsl.getIndirectReusableContainerMapBoolComparisonResultById(jcsc.getEplId());//container->location
				}
				CollectionUtils.merge(lm.getSecond(), //0->1
						cm.getSecond(), //1->4
						parentContainerMap); //0->4
				return cr;
			}			
		}
		else if(jsl.getBaseStreamCount()==jcsc.getBaseStreamCount() && jsl.getRawStreamCount()==jcsc.getRawStreamCount() &&
				!(jcsc.getAgent() instanceof JoinDelayedStreamContainer)){//FIXME: avoid two or more JoinCompatibleStreamLocationContainer
			BooleanExpressionComparisonResult agentCr=null;
			Map<EventAlias, EventAlias> jcscEplMap=new HashMap<EventAlias, EventAlias>(4);
			agentCr=check(jsl, jcsc.getAgent(), jcsc.getEplId(), jcscEplMap);
			if(agentCr!=null && agentCr.getTotalState()==State.IMPLYING){
				exprComp.setEventAliasMap(jcscEplMap);
				List<AbstractBooleanExpression> jslExtraJoinCondList=dumpImplyingAndSurplusConditionList(agentCr.getOwnPairList());
				List<AbstractBooleanExpression> jslExtraChildCondList=dumpImplyingAndSurplusConditionList(agentCr.getChildPairList());
				
				cr=beComp.compareConjunctionLists(jslExtraJoinCondList, jcsc.getExtraJoinCondList(), 0);
				if(cr.getOwnState()==State.EQUIVALENT || cr.getOwnState()==State.IMPLYING){
					BooleanExpressionComparisonResult childCr=beComp.compareConjunctionLists(jslExtraChildCondList, jcsc.getExtraChildCondList(), 0);
					cr.addChildResult(childCr);
				}
				if(cr.getTotalState()==State.EQUIVALENT || cr.getTotalState()==State.IMPLYING){					
					Map<EventAlias, EventAlias> jcscAgentEplMap=new HashMap<EventAlias, EventAlias>(4);
					agentCr=check(jsl, jcsc.getAgent(), jcsc.getAgent().getEplId(), jcscAgentEplMap);
					
					ContainerAndMapAndBoolComparisonResult cmcr=new ContainerAndMapAndBoolComparisonResult(jcsc, jcscEplMap, cr);
					ContainerAndMapAndBoolComparisonResult agentCmcr=new ContainerAndMapAndBoolComparisonResult(jcsc.getAgent(), jcscAgentEplMap, agentCr);
					ContainerAndMapAndBoolComparisonResultOfAgent cmcr2=new ContainerAndMapAndBoolComparisonResultOfAgent(cmcr, agentCmcr);
					
					if(cr.getTotalState()==State.EQUIVALENT){
						jsl.addDirectReusableContainerMapComparisonResult(cmcr2);
					}
					else{
						jsl.addIndirectReusableContainerMapComparisonResult(cmcr2);
					}
					check(jsl, jcsc.getAgent(), parentContainerEplId, parentContainerMap);
				}
			}
		}
		
		if(cr==null){
			cr=BooleanExpressionComparisonResultNone.getInstance();
		}
		addBoolComparisonResult(jsl, jcsc, cr);
		return cr;
	}
	
	public BooleanExpressionComparisonResult checkJslAndJslc(JoinStream jsl, JoinStreamContainer jsc, long parentContainerEplId,
			Map<EventAlias, EventAlias> parentContainerMap){
		BooleanExpressionComparisonResult cr=this.getBoolComparisonResult(jsl, jsc);
		if(cr!=null){
			if(cr.getTotalState()==State.NONE){
				return cr;
			}
			else{
				StreamAndMapAndBoolComparisonResult lm=jsc.getDirectOrIndirectReuseStreamMapComparisonResultByEplId(parentContainerEplId);//location->container, the location is container in parent
				ContainerAndMapAndBoolComparisonResult cm=null;
				if(cr.getTotalState()==State.EQUIVALENT){
					cm=jsl.getDirectReusableContainerMapBoolComparisonResultById(jsc.getEplId());//container->location
				}
				else if(cr.getTotalState()==State.IMPLYING){
					//parent.eplId=0, jsc.eplId=1, jsl.eplId=4
					cm=jsl.getIndirectReusableContainerMapBoolComparisonResultById(jsc.getEplId());//container->location
				}
				CollectionUtils.merge(lm.getSecond(), //0->1
						cm.getSecond(), //1->4
						parentContainerMap); //0->4
				return cr;
			}
		}
		else if(jsl.getBaseStreamCount()==jsc.getBaseStreamCount() && jsl.getRawStreamCount()==jsc.getRawStreamCount()){
			BooleanExpressionComparisonResult[] jslChildCrs=new BooleanExpressionComparisonResult[jsl.getUpStreamCount()];
			int[] jscChildFlag=new int[jsc.getUpContainerCount()];
			Arrays.fill(jscChildFlag, -1);
			int jscChildCount=0;
			
			Map<EventAlias, EventAlias> jscToJslMap=new HashMap<EventAlias, EventAlias>(4);
			for(int j=0; j<jsc.getUpContainerCount(); j++){
				if(jscChildFlag[j]>=0) continue;
				for(int i=0; i<jsl.getUpStreamCount(); i++){
					if(jslChildCrs[i]!=null && (jslChildCrs[i].getOwnState()==State.EQUIVALENT || jslChildCrs[i].getOwnState()==State.IMPLYING)){
						continue;
					}
					Map<EventAlias, EventAlias> childMap=new HashMap<EventAlias, EventAlias>(4);
					//jsl.eplId=jsl.child.eplId=4, jsc.eplId=1, jsc.child.eplId=2
					jslChildCrs[i]=check(jsl.getUpStream(i), jsc.getUpContainer(j), jsc.getEplId(), childMap);//states is children's 
					if(jslChildCrs[i]!=null && (jslChildCrs[i].getOwnState()==State.EQUIVALENT || jslChildCrs[i].getOwnState()==State.IMPLYING)){
						jscChildFlag[j]=i;
						jscChildCount++;
						jscToJslMap.putAll(childMap);//childMap: 1->4
						//check(jsl.getChild(i), jsc.getChild(j), parentContainerEplId, parentContainerMap);//parentContainerMap: parent->4
						break;
					}
				}
			}
			
			if(jscChildCount==jsc.getUpContainerCount()){
				exprComp.setEventAliasMap(jscToJslMap);
				cr=beComp.compareConjunctionLists(jsl.getJoinExprList(), jsc.getJoinExprList(), 0);
				if(cr!=null && (cr.getOwnState()==State.EQUIVALENT || cr.getOwnState()==State.IMPLYING)){
					for(int i=0; i<jslChildCrs.length; i++){
						cr.addChildResult(jslChildCrs[i]);
					}
					if(cr.getTotalState()==State.EQUIVALENT){
						jsl.addDirectReusableContainerMapComparisonResult(jsc, jscToJslMap, cr);
					}
					else if(cr.getTotalState()==State.IMPLYING){
						jsl.addIndirectReusableContainerMapComparisonResult(jsc, jscToJslMap, cr);
					}
					for(int j=0; j<jsc.getUpContainerCount(); j++){
						check(jsl.getUpStream(jscChildFlag[j]), jsc.getUpContainer(j), parentContainerEplId, parentContainerMap);
					}
				}
			}
		}
		if(cr==null){
			cr=BooleanExpressionComparisonResultNone.getInstance();
		}
		addBoolComparisonResult(jsl, jsc, cr);
		return cr;
	}
	
	public BooleanExpressionComparisonResult checkRslAndRslc(RootStream rsl, RootStreamContainer rsc, long parentContainerEplId,
			Map<EventAlias, EventAlias> parentContainerMap){
		assert(rsc.getEplId()==parentContainerEplId);
		BooleanExpressionComparisonResult cr=this.getBoolComparisonResult(rsl, rsc);
		if(cr==null){//no need to add to parentContainerMap
			if(rsl.getBaseStreamCount()==rsc.getBaseStreamCount() && rsl.getRawStreamCount()==rsc.getRawStreamCount()){
				Map<EventAlias, EventAlias> rscToRslMap=new HashMap<EventAlias, EventAlias>(4);
				BooleanExpressionComparisonResult childCr=check(rsl.getUpStream(), rsc.getUpContainer(), rsc.getEplId(), rscToRslMap);
				if(childCr!=null &&(childCr.getTotalState()==State.EQUIVALENT)){ //|| childCr.getOwnState()==State.COMPATIBLE)){
					exprComp.setEventAliasMap(rscToRslMap);
					cr=beComp.compareConjunctionLists(rsl.getWhereExprList(), rsc.getWhereExprList(), 0);
					if(cr!=null && (cr.getOwnState()==State.EQUIVALENT)){//|| cr.getOwnState()==State.COMPATIBLE)){
						cr.addChildResult(childCr);
						if(cr.getOwnState()==State.EQUIVALENT){
							rsl.addDirectReusableContainerMapComparisonResult(rsc, rscToRslMap, cr);
						}
//						else{
//							rsl.addCompatibleContainerMapComparisonResult(rsc, rscMap, cr);
//						}
					}
				}
			}
		}
		if(cr==null){
			cr=BooleanExpressionComparisonResultNone.getInstance();
		}
		addBoolComparisonResult(rsl, rsc, cr);
		return cr;
	}
	
	public BooleanExpressionComparisonResult checkPslAndPslc(PatternStream psl, PatternStreamContainer psc, long parentContainerEplId,
			Map<EventAlias, EventAlias> parentContainerMap){
		throw new RuntimeException("not implemented yet");
	}
	
	public void reset(RootStream rs){
		this.rootStream = rs;
		this.crMap.clear();
	}
	
	public void check(){
		for(RootStreamContainer rsc: this.existedRscList){
			Map<EventAlias, EventAlias> rscToRslMap=new HashMap<EventAlias, EventAlias>(4);
			this.checkRslAndRslc(rootStream, rsc, rsc.getEplId(), rscToRslMap);
		}
		checkIndividual(rootStream.getUpStream());
	}
	
	public void checkIndividual(Stream sl){
		//no XXXCompitableStreamLocation here
//		if(sl instanceof RootStream){
//			
//		}
		if(sl instanceof JoinStream){
			JoinStream jsl=(JoinStream)sl;
			for(JoinStreamContainer jsc: this.existedJscList){
				if(this.getBoolComparisonResult(jsl, jsc)==null){
					Map<EventAlias, EventAlias> jscMap=new HashMap<EventAlias, EventAlias>(4);
					if(jsc instanceof JoinDelayedStreamContainer){
						this.checkJslAndJcslc(jsl, (JoinDelayedStreamContainer)jsc, jsc.getEplId(), jscMap);
					}
					else{
						this.checkJslAndJslc(jsl, jsc, jsc.getEplId(), jscMap);
					}
				}
			}
			for(Stream child: jsl.getUpStreamList()){
				checkIndividual(child);
			}
		}
		else if(sl instanceof FilterStream){
			FilterStream fsl=(FilterStream)sl;
			for(FilterStreamContainer fsc: existedFscList){
				if(this.getBoolComparisonResult(fsl, fsc)==null){
					Map<EventAlias, EventAlias> fscMap=new HashMap<EventAlias, EventAlias>(4);
					if(fsc instanceof FilterDelayedStreamContainer){
						this.checkFslAndFcslc(fsl, (FilterDelayedStreamContainer)fsc, fsc.getEplId(), fscMap);
					}
					else{
						this.checkFslAndFslc(fsl, fsc, fsc.getEplId(), fscMap);
					}
				}
			}
		}
		else if(sl instanceof PatternStream){
			PatternStream psl=(PatternStream)sl;
			for(PatternStreamContainer psc: existedPscList){//FIXME
				if(this.getBoolComparisonResult(psl, psc)==null){
					Map<EventAlias, EventAlias> pscMap=new HashMap<EventAlias, EventAlias>(4);
					this.checkPslAndPslc(psl, psc, psc.getEplId(), pscMap);
				}
			}
		}
	}
	
	static class StreamAndContainer extends Tuple2D<DerivedStream,DerivedStreamContainer>{
		private static final long serialVersionUID = 3773336914679802713L;

		public StreamAndContainer(DerivedStream first,
				DerivedStreamContainer second) {
			super(first, second);
		}
		
		@Override
		public int hashCode(){
			return System.identityHashCode(first) ^ System.identityHashCode(second);
		}
		
		@Override
		public boolean equals(Object obj){
			if(obj instanceof StreamAndContainer){
				StreamAndContainer that=(StreamAndContainer)obj;
				return this.first==that.first && this.second==that.second;
			}
			return false;
		}
	}
	
}
