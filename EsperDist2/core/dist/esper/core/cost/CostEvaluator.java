package dist.esper.core.cost;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import dist.esper.core.cost.DeltaResourceUsage.CandidateContainerType;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.*;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResultOfAgent;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.BooleanExpressionComparisonPair;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;
import dist.esper.util.IncludingExcludingPrincipleComputer;
import dist.esper.util.MultiValueMap;

public class CostEvaluator {
	public static int INDIRECT_REUSE_MAX_COUNT=3;
	public static int NEW_MAX_COUNT=3;
	public Map<String, WorkerStat> procWorkerStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, WorkerStat> gateWorkerStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, WorkerStat> allWorkerStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, InstanceStat> containerStatMap=new ConcurrentSkipListMap<String, InstanceStat>();
	public Map<String, DerivedStreamContainer> containerNameMap;
	FilterStats filterStats;
	JoinStats joinStats;
	public RawStats rawStats;
	EventOrPropertySizeEstimator sizeEstimator;
	public MultiValueMap<String,DerivedStreamContainer> workerInputContainersMap=new MultiValueMap<String,DerivedStreamContainer>();
	public MultiValueMap<String,RawStream> workerInputRawStreamsMap=new MultiValueMap<String,RawStream>();
	
	public void registContainerRecursively(DerivedStreamContainer psc){
		if(psc instanceof RootStreamContainer){
			registContainerRecursively((DerivedStreamContainer)((RootStreamContainer)psc).getUpContainer());
		}
		else if(psc instanceof JoinDelayedStreamContainer){
			registContainerRecursively(((JoinDelayedStreamContainer) psc).getAgent());
			joinStats.registContainer((JoinDelayedStreamContainer)psc);
		}
		else if(psc instanceof JoinStreamContainer){
			for(StreamContainer child: ((JoinStreamContainer) psc).getUpContainerList()){
				registContainerRecursively((DerivedStreamContainer)child);
			}
			joinStats.registContainer((JoinStreamContainer)psc);
		}
		else if(psc instanceof FilterDelayedStreamContainer){
			registContainerRecursively(((FilterDelayedStreamContainer) psc).getAgent());
			filterStats.registContainer((FilterDelayedStreamContainer)psc);
		}
		else if(psc instanceof FilterStreamContainer){
			filterStats.registContainer((FilterStreamContainer)psc);
		}
//		else if(psc instanceof PatternStreamLocationContainer){
//			//not implemented
//		}
	}
	
	public CostEvaluator(Map<String, DerivedStreamContainer> containerMap) {
		super();
		rawStats = new RawStats();
		filterStats = new FilterStats(rawStats);
		joinStats = new JoinStats(rawStats, filterStats);
		sizeEstimator = new EventOrPropertySizeEstimator(rawStats);
		this.containerNameMap = containerMap;
		
		DeltaResourceUsage.setRawStat(rawStats);
		DeltaResourceUsage.setSizeEstimator(sizeEstimator);
		DeltaResourceUsage.setWorkerInputContainersMap(workerInputContainersMap);
		DeltaResourceUsage.setWorkerInputRawStreamsMap(workerInputRawStreamsMap);
	}

	public void updateWorkerStat(WorkerStat ws){
		if(ws.isGateway()){
			gateWorkerStatMap.put(ws.id, ws);
			procWorkerStatMap.remove(ws.id);
		}
		else{
			procWorkerStatMap.put(ws.id, ws);
			gateWorkerStatMap.remove(ws.id);
		}
		allWorkerStatMap.put(ws.id, ws);
		for(InstanceStat insStat: ws.insStats){
			containerStatMap.put(insStat.uniqueName, insStat);
			DerivedStreamContainer psc=containerNameMap.get(insStat.uniqueName);
			updateContainerStat(psc, insStat);
		}
		for(RawStreamStat rawStat: ws.rawStats){
			rawStats.updateRawStreamStat(rawStat);
		}
	}
	
	public void updateContainerStat(DerivedStreamContainer psc, InstanceStat insStat){
		if(psc instanceof RootStreamContainer){
			/*do nothing*/
		}
		else if(psc instanceof JoinStreamContainer){
			this.joinStats.updateContainerStat((JoinStreamContainer)psc, insStat);
		}
		else if(psc instanceof FilterStreamContainer){
			this.filterStats.updateContainerStat((FilterStreamContainer)psc, insStat);
		}
	}
	
	public double estimateBooleanExpressionSelectFactor(List<AbstractBooleanExpression> bExprList){
		double sf=1.0;
		for(AbstractBooleanExpression bExpr: bExprList){
			double subSF=estimateBooleanExpressionSelectFactor(bExpr);
			sf*=subSF;
		}
		return sf;
	}
	
	public double estimateBooleanExpressionSelectFactor(AbstractBooleanExpression bExpr){
		double sf=1.0;
		if(bExpr instanceof ComparisonExpression){
			ComparisonExpression ce=(ComparisonExpression)bExpr;
			if(ce.getChild(0) instanceof EventPropertySpecification && 
					ce.getChild(1) instanceof EventPropertySpecification){
				sf=joinStats.estimateSelectFactor((EventPropertySpecification)ce.getChild(0), 
						(EventPropertySpecification)ce.getChild(1), 
						ce.getRelation());
			}
			else if(ce.getChild(0) instanceof EventPropertySpecification && 
					ce.getChild(1) instanceof Value){
				sf=filterStats.estimateAbsoluteSelectFactor((EventPropertySpecification)ce.getChild(0), (Value)ce.getChild(1), ce.getRelation());
			}
		}
		else{
			CompositeExpression ce=(CompositeExpression)bExpr;
			double[] childSFs=new double[ce.getChildExprCount()];
			for(int i=0;i<ce.getChildExprCount();i++){
				childSFs[i]=estimateBooleanExpressionSelectFactor(ce.getChildExpr(i));
			}			
			if(ce.getRelation()==RelationTypeEnum.AND){
				for(int j=0;j<childSFs.length;j++){
					sf=sf*childSFs[j];
				}
			}
			else{
				sf=IncludingExcludingPrincipleComputer.compute(childSFs);
			}
		}
		return sf;
	}
	
	public double computeComparisonPairSelectFactorRatio(BooleanExpressionComparisonPair pair){
		if(pair.getState()==State.IMPLYING){
			AbstractBooleanExpression bExpr1=pair.getFirst();
			AbstractBooleanExpression bExpr2=pair.getSecond();
			double sf1=estimateBooleanExpressionSelectFactor(bExpr1);
			double sf2=estimateBooleanExpressionSelectFactor(bExpr2);
			if(sf2<=0.0){
				return sf1;
			}
			else{
				return sf1/sf2;
			}
		}
		else if(pair.getState()==State.SURPLUS){
			AbstractBooleanExpression bExpr1=pair.getFirst();
			double sf1=estimateBooleanExpressionSelectFactor(bExpr1);
			return sf1;
		}
		else if(pair.getState()==State.NONE){
			return 1.0;
		}
		else{//equal
			return 1.0;
		}
	}
	
	public double estimateSelectFactor(DerivedStreamContainer cnt, BooleanExpressionComparisonResult cr){
		double sf=1.0;
		for(BooleanExpressionComparisonPair pair: cr.getOwnPairList()){
			double pairSF=computeComparisonPairSelectFactorRatio(pair);
			sf*=pairSF;
		}
		for(BooleanExpressionComparisonPair pair: cr.getChildPairList()){
			double pairSF=computeComparisonPairSelectFactorRatio(pair);
			sf*=pairSF;
		}
		return sf;
	}
	
	public double estimateRate(Stream psl){
		return estimateRate(psl, Double.NEGATIVE_INFINITY);
	}
	
	public double estimateRate(Stream psl, double downReusedRate){
		if(psl instanceof RawStream){
			return rawStats.getOutputRateSec(((RawStream)psl));
		}
		else if(psl instanceof FilterStream){
			return estimateFilterRate((FilterStream)psl, downReusedRate);
		}
		else if(psl instanceof JoinStream){
			return estimateJoinRate((JoinStream)psl, downReusedRate);
		}
		else if(psl instanceof RootStream){
			return estimateRootRate((RootStream)psl);
		}
		return 0.0d;
	}
	
	public double estimateRateByDirectOrIndirectReusableContainer(DerivedStream psl){
		double maxRate=Double.NEGATIVE_INFINITY;
		if(psl.getDirectReusableContainerMapComparisonResultList().size()>0){
			for(ContainerAndMapAndBoolComparisonResult cmcr: psl.getDirectReusableContainerMapComparisonResultList()){
				DerivedStreamContainer psc=cmcr.getFirst();
				double newRate=containerStatMap.get(psc.getUniqueName()).getOutputRateSec();
				maxRate=newRate>maxRate?newRate:maxRate;
			}
			psl.setRate(maxRate);			
		}
		else if(psl.getIndirectReusableContainerMapComparisonResultList().size()>0){
			for(ContainerAndMapAndBoolComparisonResult cmcr: psl.getIndirectReusableContainerMapComparisonResultList()){				
				DerivedStreamContainer psc=cmcr.getFirst();
				double sf=estimateSelectFactor(psc, cmcr.getThird());
				double rate=containerStatMap.get(psc.getUniqueName()).getOutputRateSec()*sf;
				if(maxRate<rate){
					maxRate=rate;
				}
			}
			psl.setRate(maxRate);
		}
		return maxRate;
	}
	
	public double estimateRootRate(RootStream rsl){
		double rate=Double.NEGATIVE_INFINITY;
		if(rsl.getDirectReusableContainerMapComparisonResultList().size()>0){
			rate=estimateRateByDirectOrIndirectReusableContainer(rsl);
		}
		double childRate=estimateRate(rsl.getUpStream(), rate);
		rsl.setRate(rate>=childRate?rate:childRate);
		return rsl.getRate();
	}
	
	public double estimateFilterRate(FilterStream fsl, double downReusedRate){
		double rate=Double.NEGATIVE_INFINITY;
		if(fsl.getDirectReusableContainerMapComparisonResultList().size()>0 
			|| fsl.getIndirectReusableContainerMapComparisonResultList().size()>0){
			rate=estimateRateByDirectOrIndirectReusableContainer(fsl);
		}
		if(rate<0.0 && downReusedRate>=0.0){
			rate=downReusedRate;
		}
		if(rate<0.0){
			double sf=estimateBooleanExpressionSelectFactor(fsl.getFilterExpr());
			rate=rawStats.getOutputRateSec(fsl.getRawStream());
			rate*=sf;
		}
		fsl.setRate(rate);
		return fsl.getRate();
	}
	
	public double estimateJoinRate(JoinStream jsl, double downReusedRate){
		for(Stream child: jsl.getUpStreamList()){
			estimateRate(child);
		}
		
		double rate=Double.NEGATIVE_INFINITY;
		if(jsl.getDirectReusableContainerMapComparisonResultList().size()>0 
			|| jsl.getIndirectReusableContainerMapComparisonResultList().size()>0){
			rate=estimateRateByDirectOrIndirectReusableContainer(jsl);
		}
		if(rate<0.0 && downReusedRate>=0.0){
			rate=downReusedRate;
		}
		if(rate<0.0){
			rate=1.0;
			for(Stream child: jsl.getUpStreamList()){
				rate*=child.getRate();
			}
			double sf=estimateBooleanExpressionSelectFactor(jsl.getJoinExprList());
			rate=rate*sf;
		}
		jsl.setRate(rate);
		return jsl.getRate();
	}
	
	public CostMetrics computeCostMetrics(DeltaResourceUsage dru){
		CostMetrics cm=new CostMetrics();
		//dru.computeCostMetricsRecursively(cm);
		List<DeltaResourceUsage> druList=dru.dumpAll();
		cm.initWorkerResourceUsageMap(allWorkerStatMap);
		for(DeltaResourceUsage d: druList){
			cm.addDeltaWorkerResourceUsage(d.getWorkerId(), d.deltaMemoryBytes, d.deltaProcTimeUS, d.deltaOutputTimeUS);
		}
		cm.computeState();
		return cm;
	}
	
	public DeltaResourceUsage computeBestStrategy(RootStream rsl){
		this.estimateRate(rsl);
		List<DeltaResourceUsage> rootDRUList=searchRootPlan(rsl);
		return this.chooseBestOne(rootDRUList);
	}
	
	public DeltaResourceUsage chooseBestOne(List<DeltaResourceUsage> druList){
		int index=chooseBestIndex(druList);
		return druList.get(index);
	}
	
	public int chooseBestIndex(List<DeltaResourceUsage> druList){
		CostMetrics[] cms=new CostMetrics[druList.size()];
		for(int i=0;i<druList.size();i++){
			druList.get(i).compute();
			cms[i]=computeCostMetrics(druList.get(i));
			cms[i].setIndex(i);
		}
		Arrays.sort(cms,CostMetrics.comparator);
		return cms[0].getIndex();
	}

	public List<DeltaResourceUsage> searchUnrootPlan(DerivedStream psl, String parentWorkerId){
		if(psl instanceof JoinStream){
			return searchJoinPlan((JoinStream)psl, parentWorkerId);
		}
		else if(psl instanceof FilterStream){
			return searchFilterPlan((FilterStream)psl, parentWorkerId);
		}
		return null;
	}
	
	private static List<WorkerStat> getIndirectReuseLimitedWorkerStatList(
			DerivedStream ds, String parentWorkerId, 
			Map<String, WorkerStat> workerStatMap){
		List<WorkerStat> wsList=new ArrayList<WorkerStat>(INDIRECT_REUSE_MAX_COUNT+1);
		if(parentWorkerId!=null && 
				!ds.hasDirectResuableContainerOnWorker(parentWorkerId) && 
				workerStatMap.get(parentWorkerId)!=null){
			wsList.add(workerStatMap.get(parentWorkerId));
		}
		//random choose
		int n=workerStatMap.size();
		int m=INDIRECT_REUSE_MAX_COUNT;
		for(WorkerStat ws: workerStatMap.values()){
			if(!ws.getId().equals(parentWorkerId) && !ds.hasDirectResuableContainerOnWorker(ws.getId())){
				if(Math.random() < (double)m/(double)n){
					wsList.add(ws);
					m--;
				}
				if(m<=0)
					break;
			}
			n--;
			if(n<=0)
				break;
		}
		return wsList;
	}
	
	private static List<WorkerStat> getNewLimitedWorkerStatList(
			DerivedStream ds, String parentWorkerId, 
			Map<String, WorkerStat> workerStatMap){
		List<WorkerStat> wsList=new ArrayList<WorkerStat>(NEW_MAX_COUNT+1);
		if(parentWorkerId!=null && 
				!ds.hasDirectOrIndirectReusableContainerOnWorker(parentWorkerId) && 
				workerStatMap.get(parentWorkerId)!=null){
			wsList.add(workerStatMap.get(parentWorkerId));
		}
		//random choose
		int n=workerStatMap.size();
		int m=NEW_MAX_COUNT;
		for(WorkerStat ws: workerStatMap.values()){
			if(!ws.getId().equals(parentWorkerId) && !ds.hasDirectOrIndirectReusableContainerOnWorker(ws.getId())){
				if(Math.random() < (double)m/(double)n){
					wsList.add(ws);
					m--;
				}
				if(m<=0)
					break;
			}
			n--;
			if(n<=0)
				break;
		}
		return wsList;
	}
	
	public List<DeltaResourceUsage> searchFilterPlan(FilterStream fsl, String parentWorkerId){
		List<DeltaResourceUsage> druList=new ArrayList<DeltaResourceUsage>(procWorkerStatMap.size());
		if(fsl.getDirectReusableContainerMapComparisonResultList().size()>0){
			DeltaResourceUsage[] reusableDRUs=new DeltaResourceUsage[fsl.getDirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<fsl.getDirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=fsl.getDirectReusableContainerMapComparisonResultList().get(i);
				reusableDRUs[i]=computeReusableDeltaResourceUsageRecursively(fsl, cmcr);
				//FIXME: add all now
				druList.add(reusableDRUs[i]);
			}
		}
		if(fsl.getIndirectReusableContainerMapComparisonResultList().size()>0){
			DeltaResourceUsage[] agentDRUs=new DeltaResourceUsage[fsl.getIndirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<fsl.getIndirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=fsl.getIndirectReusableContainerMapComparisonResultList().get(i);
				agentDRUs[i]=computeReusableDeltaResourceUsageRecursively(fsl, cmcr);
			}
			List<WorkerStat> wsList=getIndirectReuseLimitedWorkerStatList(fsl, parentWorkerId, procWorkerStatMap);
			for(WorkerStat ws: wsList){
			//for(WorkerStat ws: this.procWorkerStatMap.values()){
				if(!fsl.hasDirectResuableContainerOnWorker(ws.id)){
					for(int i=0; i<agentDRUs.length; i++){
						DeltaResourceUsage filterCompDRU=new DeltaResourceUsage(ws, CandidateContainerType.FILTER_INDIRECT_REUSE, fsl, null);//container is set below
						filterCompDRU.addChild(agentDRUs[i]);
						filterCompDRU.setContainer(fsl.getIndirectReusableContainerMapComparisonResultList().get(i).getFirst());
						filterCompDRU.setEventAliasMap(fsl.getIndirectReusableContainerMapComparisonResultList().get(i).getSecond());
						filterCompDRU.setCompResult(fsl.getIndirectReusableContainerMapComparisonResultList().get(i).getThird());
						//FIXME: add all now
						druList.add(filterCompDRU);
					}
				}
			}
		}
//		else{
			List<WorkerStat> wsList=getNewLimitedWorkerStatList(fsl, parentWorkerId, procWorkerStatMap);
			for(WorkerStat ws: wsList){
			//for(WorkerStat ws: this.procWorkerStatMap.values()){
				if(!fsl.hasDirectOrIndirectReusableContainerOnWorker(ws.id)){
					DeltaResourceUsage filterDRU=new DeltaResourceUsage(ws, CandidateContainerType.FILTER_NEW, fsl, null);
					//FIXME: add all now
					druList.add(filterDRU);
				}
			}
//		}
		return druList;
	}
	
	public List<DeltaResourceUsage> searchJoinPlan(JoinStream jsl, String parentWorkerId){
		List<DeltaResourceUsage> druList=new ArrayList<DeltaResourceUsage>(procWorkerStatMap.size());
		if(jsl.getDirectReusableContainerMapComparisonResultList().size()>0){
			DeltaResourceUsage[] reusableDRUs=new DeltaResourceUsage[jsl.getDirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<jsl.getDirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=jsl.getDirectReusableContainerMapComparisonResultList().get(i);
				reusableDRUs[i]=computeReusableDeltaResourceUsageRecursively(jsl, cmcr);
				//FIXME: add all now
				druList.add(reusableDRUs[i]);
			}			
		}
		if(jsl.getIndirectReusableContainerMapComparisonResultList().size()>0){			
			DeltaResourceUsage[] agentDRUs=new DeltaResourceUsage[jsl.getIndirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<jsl.getIndirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=jsl.getIndirectReusableContainerMapComparisonResultList().get(i);
				agentDRUs[i]=computeReusableDeltaResourceUsageRecursively(jsl, cmcr);//JOIN_REUSE
			}
			List<WorkerStat> wsList=getIndirectReuseLimitedWorkerStatList(jsl, parentWorkerId, procWorkerStatMap);
			//for(WorkerStat ws: this.procWorkerStatMap.values()){
			for(WorkerStat ws: wsList){
				if(!jsl.hasDirectResuableContainerOnWorker(ws.id)){
					for(int i=0; i<agentDRUs.length; i++){
						DeltaResourceUsage joinCompDRU=new DeltaResourceUsage(ws, CandidateContainerType.JOIN_INDIRECT_REUSE, jsl, null);
						joinCompDRU.setContainer(agentDRUs[i].getContainer());
						joinCompDRU.addChild(agentDRUs[i]);
						joinCompDRU.setEventAliasMap(jsl.getIndirectReusableContainerMapComparisonResultList().get(i).getSecond());
						joinCompDRU.setCompResult(jsl.getIndirectReusableContainerMapComparisonResultList().get(i).getThird());
						//FIXME: add all now
						druList.add(joinCompDRU);
					}
				}
			}			
		}
//		else{
			List<WorkerStat> wsList=getNewLimitedWorkerStatList(jsl, parentWorkerId, procWorkerStatMap);
			for(WorkerStat ws: wsList){
			//for(WorkerStat ws: this.procWorkerStatMap.values()){
				if(!jsl.hasDirectOrIndirectReusableContainerOnWorker(ws.id)){
					List<DeltaResourceUsage> childDRUList0=searchUnrootPlan((DerivedStream)jsl.getUpStream(0), ws.id);//the best one for parentId=ws.id
					List<DeltaResourceUsage> childDRUList1=searchUnrootPlan((DerivedStream)jsl.getUpStream(1), ws.id);//the best one for parentId=ws.id
					for(int i=0; i<childDRUList0.size(); i++){
						for(int j=0; j<childDRUList1.size(); j++){
							DeltaResourceUsage joinDRU=new DeltaResourceUsage(ws, CandidateContainerType.JOIN_NEW, jsl);
							joinDRU.addChild(childDRUList0.get(i));
							joinDRU.addChild(childDRUList1.get(j));
							//FIXME: add all now
							druList.add(joinDRU);
						}
					}
				}
			}
//		}
		return druList;
	}
	
	public List<DeltaResourceUsage> searchRootPlan(RootStream rsl){
		List<DeltaResourceUsage> druList=new ArrayList<DeltaResourceUsage>(gateWorkerStatMap.size());
		if(rsl.getDirectReusableContainerMapComparisonResultList().size()>0){
			DeltaResourceUsage[] reusableDRUs=new DeltaResourceUsage[rsl.getDirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<rsl.getDirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=rsl.getDirectReusableContainerMapComparisonResultList().get(i);
				reusableDRUs[i]=computeReusableDeltaResourceUsageRecursively(rsl, cmcr);
				//FIXME: add all now
				druList.add(reusableDRUs[i]);
			}			
		}
//		else{
			List<WorkerStat> wsList=getNewLimitedWorkerStatList(rsl, null, gateWorkerStatMap);
			for(WorkerStat ws: wsList){
			//for(WorkerStat ws: this.gateWorkerStatMap.values()){
				if(!rsl.hasDirectResuableContainerOnWorker(ws.id)){
					List<DeltaResourceUsage> childDRUs=searchUnrootPlan((DerivedStream)rsl.getUpStream(), ws.id);//the best one for parentId=ws.id
					for(int i=0; i<childDRUs.size(); i++){
						DeltaResourceUsage rootDRU=new DeltaResourceUsage(ws, CandidateContainerType.ROOT_NEW, rsl);
						rootDRU.addChild(childDRUs.get(i));
						//FIXME: add all now
						druList.add(rootDRU);
					}
				}
			}
//		}
		return druList;
	}
	
	
	
	public DeltaResourceUsage computeReusableDeltaResourceUsageIndividually(
			DerivedStream psl,
			ContainerAndMapAndBoolComparisonResult cmcr){
		CandidateContainerType type=null;
		if(psl instanceof RootStream){
			type=CandidateContainerType.ROOT_REUSE;
		}
		else if(psl instanceof JoinStream){
			if(cmcr.getFirst() instanceof JoinDelayedStreamContainer){
				type=CandidateContainerType.JOIN_DIRECT_REUSE;
			}
			if(cmcr.getFirst() instanceof JoinStreamContainer){
				//can't be JOIN_COMPATIBLE, already handled in searchJoinPlan()
				type=CandidateContainerType.JOIN_DIRECT_REUSE;
			}
		}
		else if(psl instanceof FilterStream){
			type=CandidateContainerType.FILTER_DIRECT_REUSE;
		}
		DeltaResourceUsage dur=new DeltaResourceUsage(
				allWorkerStatMap.get(cmcr.getFirst().getWorkerId().getId()), 
				type, 
				psl, 
				cmcr.getFirst(),
				cmcr.getSecond(),
				cmcr.getThird());//FIXME
		return dur;
	}
	
	//all reuse after this
	public DeltaResourceUsage computeReusableDeltaResourceUsageRecursively(
			DerivedStream psl, 
			ContainerAndMapAndBoolComparisonResult cmcr){
		DeltaResourceUsage dru=computeReusableDeltaResourceUsageIndividually(psl, cmcr);
		if(cmcr.getFirst() instanceof RootStreamContainer){
			RootStream rsl=(RootStream)psl;
			RootStreamContainer rsc=(RootStreamContainer)cmcr.getFirst();
			
			DerivedStream rslChild=(DerivedStream)rsl.getUpStream();
			DerivedStreamContainer rscChild=(DerivedStreamContainer)rsc.getUpContainer();
			
			ContainerAndMapAndBoolComparisonResult childCmcr=rslChild.getDirectReusableContainerMapBoolComparisonResult(rscChild);
			assert(childCmcr!=null);
			
			DeltaResourceUsage childDRU=computeReusableDeltaResourceUsageRecursively(
					rslChild, childCmcr);
			dru.addChild(childDRU);
		}
		else if(cmcr.getFirst() instanceof JoinDelayedStreamContainer){
			JoinStream jsl=(JoinStream)psl;
			ContainerAndMapAndBoolComparisonResult agentCmcr=((ContainerAndMapAndBoolComparisonResultOfAgent)cmcr).getAgentContainerAndMapAndBoolComparisonResult();
			DeltaResourceUsage childDRU=computeReusableDeltaResourceUsageRecursively(
					jsl,
					agentCmcr);
			dru.addChild(childDRU);
		}
		else if(cmcr.getFirst() instanceof JoinStreamContainer){
			JoinStream jsl=(JoinStream)psl;
			JoinStreamContainer jsc=(JoinStreamContainer)cmcr.getFirst();
			for(StreamContainer jscChild: jsc.getUpContainerList()){
				DerivedStreamContainer jscPcChild=(DerivedStreamContainer)jscChild;
				for(Stream jslChild: jsl.getUpStreamList()){
					DerivedStream jslPcChild=(DerivedStream)jslChild;
					ContainerAndMapAndBoolComparisonResult childCmcr=jslPcChild.getDirectOrIndirectReusableContainerMapBoolComparisonResult(jscPcChild);
					if(childCmcr!=null){
						DeltaResourceUsage childDRU=computeReusableDeltaResourceUsageRecursively(
								jslPcChild,
								childCmcr);
						dru.addChild(childDRU);
						break;
					}
				}
			}
			assert(dru.getChildCount()==jsl.getUpStreamCount()):String.format("jsl.childCount=%d, dru.childCount=%d", jsl.getUpStreamCount(), dru.getChildCount());
//			if(dru.getChildCount()!=jsl.getChildCount()){
//				System.err.println(String.format("jsl.childCount=%d, dru.childCount=%d", jsl.getChildCount(), dru.getChildCount()));
//			}
		}
		else if(cmcr.getFirst() instanceof FilterDelayedStreamContainer){
			FilterStream fsl=(FilterStream)psl;
			ContainerAndMapAndBoolComparisonResult agentCmcr=((ContainerAndMapAndBoolComparisonResultOfAgent)cmcr).getAgentContainerAndMapAndBoolComparisonResult();
			DeltaResourceUsage childDRU=computeReusableDeltaResourceUsageRecursively(
					fsl,
					agentCmcr);
			dru.addChild(childDRU);
		}
		else if(cmcr.getFirst() instanceof FilterStreamContainer){			
			/*do nothing*/
		}
		return dru;
	}
}
