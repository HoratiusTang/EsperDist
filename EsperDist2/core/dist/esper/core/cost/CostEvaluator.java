package dist.esper.core.cost;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.esotericsoftware.minlog.Log;

import dist.esper.core.coordinator.CoordinatorStatReportor;
import dist.esper.core.coordinator.StreamReviewer;
import dist.esper.core.cost.DeltaResourceUsage.CandidateContainerType;
import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.*;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResult;
import dist.esper.core.flow.stream.DerivedStream.ContainerAndMapAndBoolComparisonResultOfAgent;
import dist.esper.core.util.ServiceManager;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.BooleanExpressionComparisonPair;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;
import dist.esper.util.IncludingExcludingPrincipleComputer;
import dist.esper.util.Logger2;
import dist.esper.util.MultiValueMap;
import dist.esper.util.ThreadUtil;
import dist.esper.util.Tuple2D;

public class CostEvaluator {
	static Logger2 log=Logger2.getLogger(CostEvaluator.class);
	public static int INDIRECT_REUSE_MAX_COUNT=3;
	public static int NEW_MAX_COUNT=2;
	public static double SMOOTH_PARAM=0.5d;
	public Map<String, WorkerStat> procWorkerStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, WorkerStat> gateWorkerStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, WorkerStat> allWorkerStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, WorkerStat> procWorkerLastStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, WorkerStat> gateWorkerLastStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, WorkerStat> allWorkerLastStatMap=new ConcurrentSkipListMap<String, WorkerStat>();
	public Map<String, InstanceStat> containerStatMap=new ConcurrentSkipListMap<String, InstanceStat>();
	public Map<String, DerivedStreamContainer> containerNameMap;
	FilterStats filterStats;
	JoinStats joinStats;
	public RawStats rawStats;
	EventOrPropertySizeEstimator sizeEstimator;
	StreamReviewer streamReviewer;
	public MultiValueMap<String,DerivedStreamContainer> workerInputContainersMap=new MultiValueMap<String,DerivedStreamContainer>();
	public MultiValueMap<String,RawStream> workerInputRawStreamsMap=new MultiValueMap<String,RawStream>();
	
	public PlanSelectionStrategy planSelectionStrategy=PlanSelectionStrategy.RANDOM;
	MixedComparator mixedComparator=new MixedComparator();
	GreedyComparator greedyComparator=new GreedyComparator();
	
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
	
	public CostEvaluator(Map<String, DerivedStreamContainer> containerMap, StreamReviewer streamReviewer) {
		super();
		rawStats = new RawStats();
		filterStats = new FilterStats(rawStats);
		joinStats = new JoinStats(rawStats, filterStats);
		sizeEstimator = new EventOrPropertySizeEstimator(rawStats);
		this.containerNameMap = containerMap;
		this.streamReviewer = streamReviewer;
		
		DeltaResourceUsage.setRawStat(rawStats);
		DeltaResourceUsage.setSizeEstimator(sizeEstimator);
		DeltaResourceUsage.setWorkerInputContainersMap(workerInputContainersMap);
		DeltaResourceUsage.setWorkerInputRawStreamsMap(workerInputRawStreamsMap);
		log.info("PlanSelectionStrategy="+planSelectionStrategy.toString());
	}
	
	public InstanceStat getContainerStat(String containerName){
		InstanceStat insStat=containerStatMap.get(containerName);
		StreamContainer cnt=containerNameMap.get(containerName);
		try{
			while(insStat==null){
				log.debug("wait to get InstanceStat for StreamContainer %s from %s", containerName, cnt.getWorkerId().getId());
				ThreadUtil.sleep(1000);
				insStat=containerStatMap.get(containerName);
			}
		}
		catch(Exception ex){
			log.error("InstanceStat is null with stream name "+containerName, ex);
		}
		return insStat;
	}
	
	private void smoothenWorkerStat(Map<String, WorkerStat> wsMap, WorkerStat ws){
		ConcurrentSkipListMap<String,WorkerStat> wsMap2=(ConcurrentSkipListMap<String,WorkerStat>)wsMap;
		WorkerStat oldWS=wsMap2.putIfAbsent(ws.id, ws);
		if(oldWS!=null){
			oldWS.cpuUsage = (1.0d - SMOOTH_PARAM) * oldWS.cpuUsage + SMOOTH_PARAM * ws.cpuUsage;
			oldWS.bwUsageUS = (1.0d - SMOOTH_PARAM) * oldWS.bwUsageUS + SMOOTH_PARAM * ws.bwUsageUS;
			oldWS.memFree = (long)((1.0d - SMOOTH_PARAM) * oldWS.memFree + SMOOTH_PARAM * ws.memFree);
			oldWS.memUsed = (long)((1.0d - SMOOTH_PARAM) * oldWS.memUsed + SMOOTH_PARAM * ws.memUsed);
		}
	}
	
	public static boolean checkWorkerOverload(WorkerStat ws){
		return ws.cpuUsage>=0.95 || //((double)ws.bwUsageUS/ServiceManager.getOutputIntervalUS())>=0.9 ||
				(double)ws.memFree/(double)ws.memUsed <= 1.0/8.0;
	}
	
	public boolean isWorkerOverload(String workerId){
		WorkerStat ws=allWorkerStatMap.get(workerId);
		if(ws!=null){
			return checkWorkerOverload(ws);
		}
		return false;
	}
	
	public int getOverloadedWorkerCount(){
		int count=0;
		for(WorkerStat ws: allWorkerStatMap.values()){
			if(checkWorkerOverload(ws)){
				count++;
			}
		}
		return count;
	}
	
	public double getOverloadedWorkerRatio(){
		return (double)getOverloadedWorkerCount()/(double)allWorkerStatMap.size();
	}
	
	public String getAllWorkerStatsDesciption(){
		StringBuilder sb=new StringBuilder();
		sb.append("{");
		for(WorkerStat ws: allWorkerStatMap.values()){
			sb.append(String.format(" %s(%s): CPU-%.2f%%, Mem-%.2f%%, BW-%.2f/%d;", 
					ws.getId(), ws.isGateway()?"gate":"proc", ws.getCpuUsage()*100, 
					(double)(ws.getMemUsed()*100)/(ws.getMemUsed()+ws.getMemFree()),
					ws.getBwUsageUS(), ServiceManager.getOutputIntervalUS()));
		}
		sb.append('}');
		return sb.toString();
	}

	public void updateWorkerStat(WorkerStat ws){
		if(ws.isGateway()){
			gateWorkerLastStatMap.put(ws.id, ws);
			smoothenWorkerStat(gateWorkerStatMap, ws);
		}
		else{
			procWorkerLastStatMap.put(ws.id, ws);
			smoothenWorkerStat(procWorkerStatMap, ws);
		}
		allWorkerLastStatMap.put(ws.id, ws);
		smoothenWorkerStat(allWorkerStatMap, ws);
		
		ws.getInsStatsLock().lock();
		log.debug("before containerStatMap.put(WorkerStat[workerId=%s])",ws.getId());
		for(InstanceStat insStat: ws.insStats){
			InstanceStat lastStat=containerStatMap.put(insStat.uniqueName, insStat);
			if(lastStat==null){
				log.debug("add new InstanceStat: workerId=%s, uniqueName=%s", ws.getId(), insStat.getUniqueName());
			}
			DerivedStreamContainer psc=containerNameMap.get(insStat.uniqueName);
			updateContainerStat(psc, insStat);
		}
		ws.getInsStatsLock().unlock();
		ws.getRawStatsLock().lock();
		for(RawStreamStat rawStat: ws.rawStats){
			rawStats.updateRawStreamStat(rawStat);
		}
		ws.getRawStatsLock().unlock();
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
				double newRate=getContainerStat(psc.getUniqueName()).getOutputRateSec();
				maxRate=newRate>maxRate?newRate:maxRate;
			}
			psl.setRate(maxRate);			
		}
		else if(psl.getIndirectReusableContainerMapComparisonResultList().size()>0){
			for(ContainerAndMapAndBoolComparisonResult cmcr: psl.getIndirectReusableContainerMapComparisonResultList()){				
				DerivedStreamContainer psc=cmcr.getFirst();
				double sf=estimateSelectFactor(psc, cmcr.getThird());
				double rate=getContainerStat(psc.getUniqueName()).getOutputRateSec()*sf;
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
	
	public StreamFlowAndDRU buildAndEvaluateAndChooseBestPlan(List<StreamFlow> sfList){
		List<DeltaResourceUsage> sfDruList=new ArrayList<DeltaResourceUsage>(sfList.size());
		for(StreamFlow sf: sfList){
			RootStream rs=sf.getRootStream();
			streamReviewer.reset(rs);
			streamReviewer.check();
			this.estimateRate(rs);
			List<DeltaResourceUsage> curSfDruList = this.searchRootPlan(rs);//DRUs for single StreamFlow
			int druIndex=this.chooseBestIndex(curSfDruList);//choose best one for current StreamFlow
			sfDruList.add(curSfDruList.get(druIndex));
		}
		int sfIndex=chooseBestIndex(sfDruList);//choose best one among all StreamFlows
		return new StreamFlowAndDRU(sfList.get(sfIndex), sfDruList.get(sfIndex));
	}
	
//	public DeltaResourceUsage computeBestStrategy(RootStream rsl){
//		this.estimateRate(rsl);
//		List<DeltaResourceUsage> rootDRUList=searchRootPlan(rsl);
//		return this.chooseBestOne(rootDRUList);
//	}
//	
//	public DeltaResourceUsage chooseBestOne(List<DeltaResourceUsage> druList){
//		int index=chooseBestIndex(druList);
//		return druList.get(index);
//	}
	
	public int chooseBestIndex(List<DeltaResourceUsage> druList){
		if(druList.size()==0){
			throw new RuntimeException("druList.size()=0");
		}
		if(planSelectionStrategy==PlanSelectionStrategy.RANDOM){
			Random rand=new Random();
			return rand.nextInt(druList.size());
		}		
		else{
			CostMetrics[] cms=new CostMetrics[druList.size()];
			for(int i=0;i<druList.size();i++){
				druList.get(i).compute(null);
				cms[i]=computeCostMetrics(druList.get(i));
				cms[i].setIndex(i);
			}
			if(planSelectionStrategy==PlanSelectionStrategy.GREEDY){
				Arrays.sort(cms, this.greedyComparator);
			}
			else{
				Arrays.sort(cms, this.mixedComparator);
			}
			return cms[0].getIndex();
		}
	}

	public List<DeltaResourceUsage> searchPlan(DerivedStream psl, String parentWorkerId){
		if(psl instanceof RootStream){
			return searchRootPlan((RootStream)psl);
		}
		else if(psl instanceof JoinStream){
			return searchJoinPlan((JoinStream)psl, parentWorkerId);
		}
		else if(psl instanceof FilterStream){
			return searchFilterPlan((FilterStream)psl, parentWorkerId);
		}
		return null;
	}
	
	/**
	private static Collection<String> getIndirectReuseLimitedWorkerStatList(
			DerivedStream ds, String parentWorkerId, 
			Map<String, WorkerStat> workerStatMap){
		Set<String> workerIdSet=new TreeSet<String>();
		if(parentWorkerId!=null && 
				!ds.hasDirectOrIndirectReusableContainerOnWorker(parentWorkerId) && 
				workerStatMap.get(parentWorkerId)!=null){
			workerIdSet.add(parentWorkerId);
		}
		for(ContainerAndMapAndBoolComparisonResult cmcr: ds.getEqualChildrenContainerMapComparisonResultList()){
			workerIdSet.add(cmcr.getFirst().getWorkerId().getId());
		}
		//random choose
		int n=workerStatMap.size();
		int m=NEW_MAX_COUNT;
		for(String workerId: workerStatMap.keySet()){
			if(!workerIdSet.contains(workerId) && !ds.hasDirectOrIndirectReusableContainerOnWorker(workerId)){
				if(Math.random() < (double)m/(double)n){
					workerIdSet.add(workerId);
					m--;
				}
				if(m<=0)
					break;
			}
			n--;
			if(n<=0)
				break;
		}
		return workerIdSet;
	}
	*/
	
	private static Collection<String> getNewLimitedWorkerStatList(
			DerivedStream ds, String parentWorkerId, 
			Map<String, WorkerStat> workerStatMap){
		Set<String> workerIdSet=new TreeSet<String>();
		if(parentWorkerId!=null && 
				!ds.hasDirectOrIndirectReusableContainerOnWorker(parentWorkerId) && 
				workerStatMap.get(parentWorkerId)!=null){
			workerIdSet.add(parentWorkerId);
		}
		for(ContainerAndMapAndBoolComparisonResult cmcr: ds.getEqualChildrenContainerMapComparisonResultList()){
			workerIdSet.add(cmcr.getFirst().getWorkerId().getId());
		}
		//random choose
		int n=workerStatMap.size();
		int m=NEW_MAX_COUNT;
		for(WorkerStat ws: workerStatMap.values()){
			if(!workerIdSet.contains(ws.id) && 
				!ds.hasDirectOrIndirectReusableContainerOnWorker(ws.id) && 
				!checkWorkerOverload(ws)){
				if(Math.random() < (double)m/(double)n){
					workerIdSet.add(ws.id);
					m--;
				}
				if(m<=0)
					break;
			}
			n--;
			if(n<=0)
				break;
		}
		return workerIdSet;
	}
	
	public List<DeltaResourceUsage> searchFilterPlan(FilterStream fsl, String parentWorkerId){
		List<DeltaResourceUsage> druList=new ArrayList<DeltaResourceUsage>(procWorkerStatMap.size());
		if(fsl.getDirectReusableContainerMapComparisonResultList().size()>0){
			DeltaResourceUsage[] reusableDRUs=new DeltaResourceUsage[fsl.getDirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<fsl.getDirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=fsl.getDirectReusableContainerMapComparisonResultList().get(i);
				if(!isWorkerOverload(cmcr.getFirst().getWorkerId().getId())){
					reusableDRUs[i]=computeReusableDeltaResourceUsageRecursively(fsl, cmcr);
					//FIXME: add all now
					druList.add(reusableDRUs[i]);
				}
			}
		}
		if(fsl.getIndirectReusableContainerMapComparisonResultList().size()>0){
			DeltaResourceUsage[] agentDRUs=new DeltaResourceUsage[fsl.getIndirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<fsl.getIndirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=fsl.getIndirectReusableContainerMapComparisonResultList().get(i);
				if(cmcr.getFirst() instanceof FilterDelayedStreamContainer ||
					isWorkerOverload(cmcr.getFirst().getWorkerId().getId())){
					agentDRUs[i]=null;
				}
				else{
					agentDRUs[i]=computeReusableDeltaResourceUsageRecursively(fsl, cmcr);
					String[] workerIds=new String[]{agentDRUs[i].getWorkerId(), parentWorkerId};
					if(agentDRUs[i].getWorkerId().equals(parentWorkerId)){
						workerIds=new String[]{parentWorkerId};
					}
					for(String workerId: workerIds){
						if(procWorkerStatMap.get(workerId)!=null){//parent might be root
							DeltaResourceUsage filterCompDRU=new DeltaResourceUsage(procWorkerStatMap.get(workerId), CandidateContainerType.FILTER_INDIRECT_REUSE, fsl, null, null);//container is set below
							filterCompDRU.addChild(agentDRUs[i]);
							filterCompDRU.setContainer(cmcr.getFirst());
							filterCompDRU.setContainerStat(getContainerStat(cmcr.getFirst().getUniqueName()));						
							filterCompDRU.setEventAliasMap(cmcr.getSecond());
							filterCompDRU.setCompResult(cmcr.getThird());						
							druList.add(filterCompDRU);
						}
					}
				}
			}
			/**
			Collection<String> workerIdList=getIndirectReuseLimitedWorkerStatList(fsl, parentWorkerId, procWorkerStatMap);
			for(String workerId: workerIdList){			
				if(!fsl.hasDirectResuableContainerOnWorker(workerId)){
					for(int i=0; i<agentDRUs.length; i++){
						if(agentDRUs[i]==null){
							continue;
						}
						DeltaResourceUsage filterCompDRU=new DeltaResourceUsage(procWorkerStatMap.get(workerId), CandidateContainerType.FILTER_INDIRECT_REUSE, fsl, null);//container is set below
						filterCompDRU.addChild(agentDRUs[i]);
						filterCompDRU.setContainer(fsl.getIndirectReusableContainerMapComparisonResultList().get(i).getFirst());
						filterCompDRU.setEventAliasMap(fsl.getIndirectReusableContainerMapComparisonResultList().get(i).getSecond());
						filterCompDRU.setCompResult(fsl.getIndirectReusableContainerMapComparisonResultList().get(i).getThird());
						//FIXME: add all now
						druList.add(filterCompDRU);
					}
				}
			}*/
		}
//		else{
			Collection<String> workerIdList=getNewLimitedWorkerStatList(fsl, parentWorkerId, procWorkerStatMap);
			for(String workerId: workerIdList){				
				if(!fsl.hasDirectOrIndirectReusableContainerOnWorker(workerId)){
					DeltaResourceUsage filterDRU=new DeltaResourceUsage(procWorkerStatMap.get(workerId), CandidateContainerType.FILTER_NEW, fsl, null, null);
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
				if(!isWorkerOverload(cmcr.getFirst().getWorkerId().getId())){
					reusableDRUs[i]=computeReusableDeltaResourceUsageRecursively(jsl, cmcr);
					//FIXME: add all now
					druList.add(reusableDRUs[i]);
				}
			}			
		}
		if(jsl.getIndirectReusableContainerMapComparisonResultList().size()>0){			
			DeltaResourceUsage[] agentDRUs=new DeltaResourceUsage[jsl.getIndirectReusableContainerMapComparisonResultList().size()];
			for(int i=0; i<jsl.getIndirectReusableContainerMapComparisonResultList().size(); i++){
				ContainerAndMapAndBoolComparisonResult cmcr=jsl.getIndirectReusableContainerMapComparisonResultList().get(i);
				if(cmcr.getFirst() instanceof JoinDelayedStreamContainer ||
					isWorkerOverload(cmcr.getFirst().getWorkerId().getId())){
					agentDRUs[i]=null;
				}
				else{
					agentDRUs[i]=computeReusableDeltaResourceUsageRecursively(jsl, cmcr);//JOIN_REUSE
					String[] workerIds=new String[]{agentDRUs[i].getWorkerId(), parentWorkerId};
					if(agentDRUs[i].getWorkerId().equals(parentWorkerId)){
						workerIds=new String[]{parentWorkerId};
					}
					for(String workerId: workerIds){
						if(procWorkerStatMap.get(workerId)!=null){//parent might be root
							DeltaResourceUsage joinCompDRU=new DeltaResourceUsage(procWorkerStatMap.get(workerId), CandidateContainerType.JOIN_INDIRECT_REUSE, jsl, null, null);
							joinCompDRU.addChild(agentDRUs[i]);
							joinCompDRU.setContainer(agentDRUs[i].getContainer());
							joinCompDRU.setContainerStat(getContainerStat(agentDRUs[i].getContainer().getUniqueName()));
							joinCompDRU.setEventAliasMap(cmcr.getSecond());
							joinCompDRU.setCompResult(cmcr.getThird());
							druList.add(joinCompDRU);
						}
					}
				}
			}
			/**
			Collection<String> workerIdList=getIndirectReuseLimitedWorkerStatList(jsl, parentWorkerId, procWorkerStatMap);			
			for(String workerId: workerIdList){
				if(!jsl.hasDirectResuableContainerOnWorker(workerId)){
					for(int i=0; i<agentDRUs.length; i++){
						if(agentDRUs[i]==null){
							continue;
						}
						DeltaResourceUsage joinCompDRU=new DeltaResourceUsage(procWorkerStatMap.get(workerId), CandidateContainerType.JOIN_INDIRECT_REUSE, jsl, null);
						joinCompDRU.setContainer(agentDRUs[i].getContainer());
						joinCompDRU.addChild(agentDRUs[i]);
						joinCompDRU.setEventAliasMap(jsl.getIndirectReusableContainerMapComparisonResultList().get(i).getSecond());
						joinCompDRU.setCompResult(jsl.getIndirectReusableContainerMapComparisonResultList().get(i).getThird());
						//FIXME: add all now
						druList.add(joinCompDRU);
					}
				}
			}
			*/		
		}
//		else{
			Collection<String> workerIdList=getNewLimitedWorkerStatList(jsl, parentWorkerId, procWorkerStatMap);
			for(String workerId: workerIdList){			
				if(!jsl.hasDirectOrIndirectReusableContainerOnWorker(workerId)){
					List<DeltaResourceUsage> childDRUList0=searchPlan((DerivedStream)jsl.getUpStream(0), workerId);//the best one for parentId=ws.id
					List<DeltaResourceUsage> childDRUList1=searchPlan((DerivedStream)jsl.getUpStream(1), workerId);//the best one for parentId=ws.id
					for(int i=0; i<childDRUList0.size(); i++){
						DeltaResourceUsage childDRU0=childDRUList0.get(i); 
						for(int j=0; j<childDRUList1.size(); j++){
							DeltaResourceUsage childDRU1=childDRUList1.get(j);
							if(childDRU0.getWorkerId().equals(workerId) || 
									childDRU1.getWorkerId().equals(workerId)){
								DeltaResourceUsage joinDRU=new DeltaResourceUsage(procWorkerStatMap.get(workerId), CandidateContainerType.JOIN_NEW, jsl);
								joinDRU.addChild(childDRU0);
								joinDRU.addChild(childDRU1);
								druList.add(joinDRU);
							}
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
				if(!isWorkerOverload(cmcr.getFirst().getWorkerId().getId())){
					reusableDRUs[i]=computeReusableDeltaResourceUsageRecursively(rsl, cmcr);
					//FIXME: add all now
					druList.add(reusableDRUs[i]);
				}
			}			
		}
//		else{
			Collection<String> workerIdList=getNewLimitedWorkerStatList(rsl, null, gateWorkerStatMap);
			for(String workerId: workerIdList){
				if(!rsl.hasDirectResuableContainerOnWorker(workerId)){
					List<DeltaResourceUsage> childDRUs=searchPlan((DerivedStream)rsl.getUpStream(), workerId);//the best one for parentId=ws.id
					for(int i=0; i<childDRUs.size(); i++){
						DeltaResourceUsage rootDRU=new DeltaResourceUsage(gateWorkerStatMap.get(workerId), CandidateContainerType.ROOT_NEW, rsl);
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
				getContainerStat(cmcr.getFirst().getUniqueName()),
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
	
	public static class StreamFlowAndDRU extends Tuple2D<StreamFlow, DeltaResourceUsage>{
		public StreamFlowAndDRU(StreamFlow streamFlow,
				DeltaResourceUsage deltaResourceUsage) {
			super(streamFlow, deltaResourceUsage);
		}

		private static final long serialVersionUID = 6354577762750365549L;		
	}
	
	public enum PlanSelectionStrategy{
		RANDOM("random"),
		GREEDY("greedy"),
		MIXED("mixed");
		
		String str;
		private PlanSelectionStrategy(String str){
			this.str=str;
		}
		@Override
		public String toString(){
			return str;
		}
	}
	
	public static class MixedComparator implements Comparator<CostMetrics>{
		@Override
		public int compare(CostMetrics a, CostMetrics b) {
			double out1=a.deltaOutputTimeUS * Math.sqrt(a.outputTimeUSVariance);
			double cpu1=a.deltaCPUTimeUS * Math.sqrt(a.cpuTimeUSVariance);
			double mem1=a.deltaMemoryBytes * Math.sqrt(a.memoryBytesVariance);
			
			double out2=b.deltaOutputTimeUS * Math.sqrt(b.outputTimeUSVariance);
			double cpu2=b.deltaCPUTimeUS * Math.sqrt(b.cpuTimeUSVariance);
			double mem2=b.deltaMemoryBytes * Math.sqrt(b.memoryBytesVariance);
			
			double outAvg=(out1+out2)/2;
			double cpuAvg=(cpu1+cpu2)/2;
			double memAvg=(mem1+mem2)/2;
			
			outAvg=(outAvg==0)?Double.MIN_NORMAL:outAvg;
			cpuAvg=(cpuAvg==0)?Double.MIN_NORMAL:cpuAvg;
			memAvg=(memAvg==0)?Double.MIN_NORMAL:memAvg;
			
			double outR1=out1/outAvg;
			double outR2=out2/outAvg;			
			double cpuR1=cpu1/cpuAvg;
			double cpuR2=cpu2/cpuAvg;
			double memR1=mem1/memAvg;
			double memR2=mem2/memAvg;
			
			double alpha=1.0d, beta=1.0d, gamma=1.0d;
			
			double v1= outR1 * outR1 * alpha + cpuR1 * cpuR1 * beta + memR1 * memR1 * gamma;
			double v2= outR2 * outR2 * alpha + cpuR2 * cpuR2 * beta + memR2 * memR2 * gamma;
			
			int comp=Double.compare(v1, v2);
			return 0-comp;
			/**
			if(out1!=out2){
				return out1>out2?1:-1;
			}
			else if(cpu1!=cpu2){
				return cpu1>cpu2?1:-1;
			}
			else if(mem1!=mem2){
				return mem1>mem2?1:-1;
			}*/
		}
	}
	
	public static class GreedyComparator implements Comparator<CostMetrics>{
		@Override
		public int compare(CostMetrics a, CostMetrics b) {
			int cpuComp=Double.compare(a.deltaCPUTimeUS, b.deltaCPUTimeUS);
			if(cpuComp!=0){
				return cpuComp;
			}
			int memComp=Double.compare(a.deltaMemoryBytes, b.deltaMemoryBytes);
			if(memComp!=0){
				return memComp;
			}
			int outComp=Double.compare(a.deltaOutputTimeUS, b.deltaOutputTimeUS);
			if(outComp!=0){
				return outComp;
			}
			return 0;
		}
	}
}
