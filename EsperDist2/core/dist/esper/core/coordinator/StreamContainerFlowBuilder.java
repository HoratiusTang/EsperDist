package dist.esper.core.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.core.CoordinatorMain;
import dist.esper.core.coordinator.Coordinator.WorkerAssignmentStrategy;
import dist.esper.core.cost.*;
import dist.esper.core.cost.DeltaResourceUsage.CandidateContainerType;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.container.DerivedStreamContainer.StreamAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.*;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResult;
import dist.esper.core.id.WorkerId;
import dist.esper.core.util.ServiceManager;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.util.Logger2;

/**
 * 
 * 
 * @author tjy
 *
 */
public class StreamContainerFlowBuilder {
	static Logger2 log=Logger2.getLogger(StreamContainerFlowBuilder.class);
	Coordinator coordinator;
	StreamReviewer streamReviewer;
	
	public StreamContainerFlowBuilder(Coordinator coordinator) {
		super();
		this.coordinator = coordinator;
		streamReviewer=new StreamReviewer(
				coordinator.existedFscList,
				coordinator.existedPscList,
				coordinator.existedJscList,
				coordinator.existedRscList);
	}
	
	public StreamContainerFlow buildStreamContainerFlow(StreamFlow sf, DeltaResourceUsage rootDRU){
		assert(rootDRU.getStream()==sf.getRootStream());
		//log.info("StreamFlow: \n%s", sf.toString());
		RootStream rootStream=(RootStream)insertDelayedStreamByDeltaResourceUsageIfNeeded(rootDRU);
		//log.info("StreamFlow: \n%s", sf.toString());
		//RootStreamContainer rootContainer=(RootStreamContainer)buildStreamContainerRecursively2(sf.getRootStream(), false);
		RootStreamContainer rootContainer=buildRootStreamContainerRecursively2((RootStream)sf.getRootStream());
		
		assginUniqueName(rootContainer);
		StreamContainerFlow sct=new StreamContainerFlow(sf.getEplId(), sf.getEpl(), rootStream, rootContainer, rootDRU);


		//log.info("\n-----------------------------------------------");
		log.info("StreamFlow: \n%s", sf.toString());
		//log.info("-----------------------------------------------");
		log.info("StreamContainerFlow: \n%s", sct.toString());
		//log.info("-----------------------------------------------\n");
		return sct;
	}

	public Stream insertDelayedStreamByDeltaResourceUsageIfNeeded(DeltaResourceUsage dru){
		if(dru.getStream() instanceof RootStream){//BRANCH 1
			RootStream rsl=(RootStream)dru.getStream();
			if(dru.getType()==CandidateContainerType.ROOT_REUSE){//BRANCH 11
				RootStreamContainer rsc=(RootStreamContainer)dru.getContainer();
				BooleanExpressionComparisonResult cr=dru.getCompResult();
				Map<EventAlias, EventAlias> eaMap=dru.getEaMap();
				rsl.setFinalReusingContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(rsc, eaMap, cr));
			}
			else{//BRANCH 12
				rsl.setWorkerId(ServiceManager.getInstance(coordinator.id).getWorkerId(dru.getWorkerId()));
			}
			Stream newChild=insertDelayedStreamByDeltaResourceUsageIfNeeded(dru.getChildList().get(0));
			rsl.setUpStream(newChild);
			return rsl;
		}
		else if(dru.getStream() instanceof JoinStream){//BRANCH 2
			JoinStream jsl=(JoinStream)dru.getStream();
			JoinDelayedStream jcsl=null;
			if(dru.getType()==CandidateContainerType.JOIN_INDIRECT_REUSE){//BRANCH 21. dru's child must be JOIN_REUSE & JoinStream
				JoinStreamContainer jsc=(JoinStreamContainer)dru.getContainer();
				BooleanExpressionComparisonResult cr=dru.getCompResult();
				Map<EventAlias, EventAlias> eaMap=dru.getEaMap();
				jcsl=new JoinDelayedStream(jsl, cr);				
				
				jcsl.setFinalReusingContainerMapComparisonResult(null);
				jcsl.setWorkerId(ServiceManager.getInstance(coordinator.id).getWorkerId(dru.getWorkerId()));
				
				JoinStream agent=(JoinStream)insertDelayedStreamByDeltaResourceUsageIfNeeded(dru.getChildList().get(0));
				assert(agent==jsl);
				jcsl.setAgent(agent);
				
				Set<EventOrPropertySpecification> extraCondEpsSet=jcsl.dumpExtraConditionEventOrPropertySpecReferences();
				for(EventOrPropertySpecification extraCondEps: extraCondEpsSet){
					jsl.tryAddResultElement(extraCondEps);
				}
				jsl.setFinalReusingContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(jsc, eaMap, cr));//FIXME
			}
			else if(dru.getType()==CandidateContainerType.JOIN_DIRECT_REUSE){//BRANCH 22
				if(dru.getContainer() instanceof JoinDelayedStreamContainer){//BRANCH 221
					JoinDelayedStreamContainer jcsc=(JoinDelayedStreamContainer)dru.getContainer();
					BooleanExpressionComparisonResult cr=dru.getCompResult();
					Map<EventAlias, EventAlias> eaMap=dru.getEaMap();
					jcsl=new JoinDelayedStream(jsl, cr);
					jcsl.setFinalReusingContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(jcsc, eaMap, cr));
					JoinStream agent=(JoinStream)insertDelayedStreamByDeltaResourceUsageIfNeeded(dru.getChildList().get(0));//go to 2
					assert(agent==jsl);
					jcsl.setAgent(jsl);
				}
				else if(dru.getContainer() instanceof JoinStreamContainer){//BRANCH 222
					JoinStreamContainer jsc=(JoinStreamContainer)dru.getContainer();
					BooleanExpressionComparisonResult cr=dru.getCompResult();
					Map<EventAlias, EventAlias> eaMap=dru.getEaMap();
					jsl.setFinalReusingContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(jsc, eaMap, cr));						
				}
			}
			else{//BRANCH 23
				jsl.setWorkerId(ServiceManager.getInstance(coordinator.id).getWorkerId(dru.getWorkerId()));
			}
			if(jcsl==null){
				List<Stream> newChildList=new ArrayList<Stream>(jsl.getUpStreamList().size());
				for(DeltaResourceUsage childDRU: dru.getChildList()){
					Stream newChild=insertDelayedStreamByDeltaResourceUsageIfNeeded(childDRU);
					newChildList.add(newChild);
				}
				jsl.getUpStreamList().clear();
				jsl.setUpStreamList(newChildList);
				return jsl;
			}
			else{
				return jcsl;
			}
		}
		else if(dru.getStream() instanceof FilterStream){//BRANCH 3
			FilterStream fsl=(FilterStream)dru.getStream();
			FilterDelayedStream fcsl=null;
			if(dru.getType()==CandidateContainerType.FILTER_INDIRECT_REUSE){//BRANCH 31
				FilterStreamContainer fsc=(FilterStreamContainer)dru.getContainer();
				BooleanExpressionComparisonResult cr=dru.getCompResult();
				Map<EventAlias, EventAlias> eaMap=dru.getEaMap();
				fcsl=new FilterDelayedStream(fsl, cr);

				fcsl.setFinalReusingContainerMapComparisonResult(null);
				fcsl.setWorkerId(ServiceManager.getInstance(coordinator.id).getWorkerId(dru.getWorkerId()));
				
				FilterStream agent=(FilterStream)insertDelayedStreamByDeltaResourceUsageIfNeeded(dru.getChildList().get(0));
				assert(agent==fsl);
				fcsl.setAgent(agent);
				
				Set<EventOrPropertySpecification> extraCondEpsSet=fcsl.dumpExtraConditionEventOrPropertySpecReferences();
				for(EventOrPropertySpecification extraCondEps: extraCondEpsSet){
					fsl.tryAddResultElement(extraCondEps);
				}
				fsl.setFinalReusingContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(fsc, eaMap, cr));//FIXME
			}
			else if(dru.getType()==CandidateContainerType.FILTER_DIRECT_REUSE){//BRANCH 32
				if(dru.getContainer() instanceof FilterDelayedStreamContainer){//BRANCH 321
					FilterDelayedStreamContainer fcsc=(FilterDelayedStreamContainer)dru.getContainer();
					BooleanExpressionComparisonResult cr=dru.getCompResult();
					Map<EventAlias, EventAlias> eaMap=dru.getEaMap();
					fcsl=new FilterDelayedStream(fsl, cr);
					fcsl.setFinalReusingContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(fcsc, eaMap, cr));
					FilterStream agent=(FilterStream)insertDelayedStreamByDeltaResourceUsageIfNeeded(dru.getChildList().get(0));
					assert(agent==fsl);
					fcsl.setAgent(fsl);
				}
				else if(dru.getContainer() instanceof FilterStreamContainer){//BRANCH 322
					FilterStreamContainer fsc=(FilterStreamContainer)dru.getContainer();
					BooleanExpressionComparisonResult cr=dru.getCompResult();
					Map<EventAlias, EventAlias> eaMap=dru.getEaMap();
					fsl.setFinalReusingContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(fsc, eaMap, cr));						
				}
			}
			else{//BRANCH 33
				WorkerId workerId=ServiceManager.getInstance(coordinator.id).getWorkerId(dru.getWorkerId());
				if(workerId==null){
					log.error("WorkerId is null by workerId=%s, workerIdMap=%s",dru.getWorkerId(), ServiceManager.getInstance(coordinator.id).getWorkerIdMap().toString());
				}
				fsl.setWorkerId(workerId);
			}
			return (fcsl==null)?fsl:fcsl;
		}
		return null;
	}
	
	public RootStreamContainer buildRootStreamContainerRecursively2(RootStream rs){
		//StreamContainer.streamContainersLock.lock();
		RootStreamContainer rsc=(RootStreamContainer)buildStreamContainerRecursively2(rs, false);
		//StreamContainer.streamContainersLock.unlock();
		return rsc;
	}
	
	public StreamContainer buildStreamContainerRecursively2(Stream sl, boolean alreadyIndirectReused){
		if(sl instanceof RootStream){
			RootStream rsl=(RootStream)sl;
			StreamContainer childContainer=buildStreamContainerRecursively2(rsl.getUpStream(), alreadyIndirectReused);
			RootStreamContainer rootContainer=null;
			if(rsl.getFinalReusingContainerMapComparisonResult()!=null){
				ContainerAndMapAndBoolComparisonResult cmcr=rsl.getFinalReusingContainerMapComparisonResult();
				rootContainer=(RootStreamContainer)cmcr.getFirst();
				rootContainer.setUpContainer(childContainer);//childContainer may be old
				rootContainer.merge(rsl, cmcr.getSecond(), cmcr.getThird());
			}
			else{
				rootContainer=new RootStreamContainer(rsl);
				rootContainer.setUpContainer(childContainer);//childContainer may be old
				StreamAndMapAndBoolComparisonResult lmcr=StreamFactory.genContainerMapComparisonResult(rsl);
				rootContainer.merge(rsl, lmcr.getSecond(), lmcr.getThird());
			}
			return rootContainer;
		}
		else if(sl instanceof JoinDelayedStream){
			JoinDelayedStream jcsl=(JoinDelayedStream)sl;
			JoinStreamContainer jslContainer=(JoinStreamContainer)buildStreamContainerRecursively2(jcsl.getAgent(), true);
			if(alreadyIndirectReused && jcsl.getFinalReusingContainerMapComparisonResult()==null){
				return jslContainer;
			}
			else{
				JoinDelayedStreamContainer jcslContainer=null;
				if(jcsl.getFinalReusingContainerMapComparisonResult()!=null){//existing JoinDelayedStreamContainer
					ContainerAndMapAndBoolComparisonResult cmcr=jcsl.getFinalReusingContainerMapComparisonResult();
					jcslContainer=(JoinDelayedStreamContainer)cmcr.getFirst();
					jcslContainer.setAgent(jslContainer);
					jcslContainer.merge(jcsl, cmcr.getSecond(), cmcr.getThird());
				}
				else{//no existing JoinDelayedStreamContainer, should create one
					jcslContainer=new JoinDelayedStreamContainer(jcsl, jslContainer);
					jcslContainer.setAgent(jslContainer);
					StreamAndMapAndBoolComparisonResult lmcr=StreamFactory.genContainerMapComparisonResult(jcsl);
					jcslContainer.merge(jcsl, lmcr.getSecond(), lmcr.getThird());
				}
				return jcslContainer;
			}
		}
		else if(sl instanceof JoinStream){
			JoinStream jsl=(JoinStream)sl;
			List<StreamContainer> childContainerList=new ArrayList<StreamContainer>(jsl.getUpStreamList().size());
			for(Stream child: jsl.getUpStreamList()){
				StreamContainer childContainer=buildStreamContainerRecursively2(child, alreadyIndirectReused);
				childContainerList.add(childContainer);
			}
			JoinStreamContainer joinContainer=null;
			if(jsl.getFinalReusingContainerMapComparisonResult()!=null){//FIXME: might be JoinDelayedContainer
				ContainerAndMapAndBoolComparisonResult cmcr=jsl.getFinalReusingContainerMapComparisonResult();
				joinContainer=(JoinStreamContainer)cmcr.getFirst();
				joinContainer.addUpContainersIfNotExist(childContainerList);
				joinContainer.merge(jsl, cmcr.getSecond(), cmcr.getThird());
			}
			else{
				joinContainer=new JoinStreamContainer(jsl);
				joinContainer.addUpContainersIfNotExist(childContainerList);
				StreamAndMapAndBoolComparisonResult lmcr=StreamFactory.genContainerMapComparisonResult(jsl);
				joinContainer.merge(jsl, lmcr.getSecond(), lmcr.getThird());
			}
			return joinContainer;
		}
		else if(sl instanceof FilterDelayedStream){
			FilterDelayedStream fcsl=(FilterDelayedStream)sl;				
			FilterStreamContainer fslContainer=(FilterStreamContainer)buildStreamContainerRecursively2(fcsl.getAgent(), true);
			
			if(alreadyIndirectReused && fcsl.getFinalReusingContainerMapComparisonResult()==null){
				return fslContainer;
			}
			else{
				FilterDelayedStreamContainer fcslContainer=null;
				if(fcsl.getFinalReusingContainerMapComparisonResult()!=null){
					ContainerAndMapAndBoolComparisonResult cmcr=fcsl.getFinalReusingContainerMapComparisonResult();
					fcslContainer=(FilterDelayedStreamContainer)cmcr.getFirst();
					fcslContainer.setAgent(fslContainer);
					fcslContainer.merge(fcsl, cmcr.getSecond(), cmcr.getThird());
				}
				else{
					fcslContainer=new FilterDelayedStreamContainer(fcsl, fslContainer);
					StreamAndMapAndBoolComparisonResult lmcr=StreamFactory.genContainerMapComparisonResult(fcsl);
					fcslContainer.setAgent(fslContainer);
					fcslContainer.merge(fcsl, lmcr.getSecond(), lmcr.getThird());
				}
				return fcslContainer;
			}
		}
		else if(sl instanceof FilterStream){
			FilterStream fsl=(FilterStream)sl;
			FilterStreamContainer filterContainer=null;
			if(fsl.getFinalReusingContainerMapComparisonResult()!=null){
				ContainerAndMapAndBoolComparisonResult cmcr=fsl.getFinalReusingContainerMapComparisonResult();
				filterContainer=(FilterStreamContainer)cmcr.getFirst();
				filterContainer.merge(fsl, cmcr.getSecond(), cmcr.getThird());
			}
			else{
				filterContainer=new FilterStreamContainer(fsl);
				StreamAndMapAndBoolComparisonResult lmcr=StreamFactory.genContainerMapComparisonResult(fsl);
				filterContainer.merge(fsl, lmcr.getSecond(), lmcr.getThird());
			}
			return filterContainer;
		}
		else if(sl instanceof PatternStream){
			PatternStream psl=(PatternStream)sl;
			PatternStreamContainer patternContainer=null;
			if(psl.getFinalReusingContainerMapComparisonResult()!=null){
				ContainerAndMapAndBoolComparisonResult cmcr=psl.getFinalReusingContainerMapComparisonResult();
				patternContainer=(PatternStreamContainer)cmcr.getFirst();
				patternContainer.merge(psl, cmcr.getSecond(), cmcr.getThird());
			}
			else{
				patternContainer=new PatternStreamContainer(psl);
				StreamAndMapAndBoolComparisonResult lmcr=StreamFactory.genContainerMapComparisonResult(psl);
				patternContainer.merge(psl, lmcr.getSecond(), lmcr.getThird());
			}
			return patternContainer;
		}
		return null;
	}
	
	public void assginUniqueName(StreamContainer sc){			
		if(sc instanceof RootStreamContainer){
			RootStreamContainer rsc=(RootStreamContainer)sc;
			assginUniqueName(rsc.getUpContainer());
			if(rsc.getUniqueName()==null){					
				rsc.setUniqueName(nextStreamUniqueName(rsc));
			}
		}
		else if(sc instanceof JoinDelayedStreamContainer){
			JoinDelayedStreamContainer jcsc=(JoinDelayedStreamContainer)sc;
			assginUniqueName(jcsc.getAgent());
			if(jcsc.getUniqueName()==null){
				jcsc.setUniqueName(nextStreamUniqueName(jcsc));
			}
		}
		else if(sc instanceof JoinStreamContainer){
			JoinStreamContainer jsc=(JoinStreamContainer)sc;
			for(StreamContainer csl: jsc.getUpContainerList()){
				assginUniqueName(csl);
			}
			if(jsc.getUniqueName()==null){					
				jsc.setUniqueName(nextStreamUniqueName(jsc));
			}
		}
		else if(sc instanceof FilterDelayedStreamContainer){
			FilterDelayedStreamContainer fcsc=(FilterDelayedStreamContainer)sc;
			assginUniqueName(fcsc.getAgent());
			if(fcsc.getUniqueName()==null){					
				fcsc.setUniqueName(nextStreamUniqueName(fcsc));
			}
		}
		else if(sc instanceof FilterStreamContainer){
			FilterStreamContainer fsc=(FilterStreamContainer)sc;
			if(fsc.getUniqueName()==null){
				fsc.setUniqueName(nextStreamUniqueName(fsc));
			}
		}
		else if(sc instanceof PatternStreamContainer){
			PatternStreamContainer psc=(PatternStreamContainer)sc;
			if(psc.getUniqueName()==null){
				psc.setUniqueName(nextStreamUniqueName(psc));
			}
		}			
		assginUniqueNameToSelectElement(sc);			
	}
	
	public void assginUniqueNameToSelectElement(StreamContainer sc){
		for(SelectClauseExpressionElement se: sc.getResultElementList()){
			if(se.getUniqueName()==null){//maybe assigned by ProcessedStreamLocation.reuse()
				se.setUniqueName(nextSelectElementUniqueName());
			}
		}
		
		if(sc instanceof RootStreamContainer){
			RootStreamContainer rsc=(RootStreamContainer)sc;
			assginUniqueNameToSelectElement(rsc.getUpContainer());
		}
		else if(sc instanceof JoinDelayedStreamContainer){
			JoinDelayedStreamContainer jcsc=(JoinDelayedStreamContainer)sc;
			assginUniqueNameToSelectElement(jcsc.getAgent());
		}
		else if(sc instanceof JoinStreamContainer){
			JoinStreamContainer jsl=(JoinStreamContainer)sc;
			for(StreamContainer csl: jsl.getUpContainerList()){
				assginUniqueNameToSelectElement(csl);
			}
		}
		else if(sc instanceof FilterDelayedStreamContainer){
			FilterDelayedStreamContainer fcsc=(FilterDelayedStreamContainer)sc;
			assginUniqueNameToSelectElement(fcsc.getAgent());
		}
	}
	
	public String nextSelectElementUniqueName(){
		return "se_"+coordinator.selectElementUID.getAndIncrement();
	}
	
//	public String nextStreamUniqueName(){
//		return "sl_"+coordinator.streamUID.getAndIncrement();
//	}
	
	public String nextStreamUniqueName(DerivedStreamContainer dsc){
		long sid=coordinator.streamUID.getAndIncrement();
		if(dsc instanceof RootStreamContainer){
			return "RT"+sid;
		}
		else if(dsc instanceof JoinDelayedStreamContainer){
			return "JD"+sid;
		}
		else if(dsc instanceof JoinStreamContainer){
			return "JN"+sid;
		}
		else if(dsc instanceof FilterDelayedStreamContainer){
			return "FD"+sid;
		}
		else if(dsc instanceof FilterStreamContainer){
			return "FT"+sid;
		}
		return null;
	}
}
