package dist.esper.core.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.core.flow.container.*;
import dist.esper.core.flow.stream.*;
import dist.esper.core.util.ServiceManager;
import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.EventOrPropertySpecReferenceDumper;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.BooleanExpressionComparisonPair;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator.*;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator;
import dist.esper.util.MultiValueMap;
import dist.esper.util.StringUtil;

public class DeltaResourceUsage{
	public static EventOrPropertySizeEstimator sizeEstimator;
	public static MultiValueMap<String,DerivedStreamContainer> workerInputContainersMap;
	public static MultiValueMap<String,RawStream> workerInputRawStreamsMap;
	public static RawStats rawStats;
	public static EventOrPropertySpecComparator epsComparator=new EventOrPropertySpecComparator();
	//public long eplId;
	CandidateContainerType type;
	//public String workerId;
	public WorkerStat workerStat;
	//public long containerId; //if new, it's -1; else, it's the reused or compated container id
	public DerivedStream stream;
	public DerivedStreamContainer container;//null if new
	public Map<EventAlias, EventAlias> eaMap;
	public BooleanExpressionComparisonResult compResult;//only compatible
	//public double outputRate;//count per ms
	//public long outputIntervalUS=2000000;//us
	
	public double deltaMemoryBytes;//bytes=sum(child.deltaOutputBytesSize)
	public double deltaProcTimeUS;//us per s
	public double deltaOutputBytesPerEvent;
	public double deltaOutputTimeUS;//us per s
	public List<SelectClauseExpressionElement> ereList;
	List<EventOrPropertySpecification> parentExtraCEPSList=Collections.emptyList();
	public List<DeltaResourceUsage> childList=new ArrayList<DeltaResourceUsage>(2);
	//private CostMetrics costMetrics;
	//public static Comparator<DeltaResourceUsage> comparator=new DefaultComparator();
	
	public String getWorkerId(){
		return workerStat.id;
	}
	
	public void toStringBuilder(StringBuilder sb, int indent){
		sb.append(StringUtil.getSpaces(indent));
		sb.append(String.format("DeltaResourceUsage[%s, %d, %s, %s]\n", 
				type.toString(), stream.getId(), workerStat.id, container==null?"null":container.getId()));
		for(DeltaResourceUsage child: childList){
			child.toStringBuilder(sb, indent+4);
		}
	}
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		this.toStringBuilder(sb,0);
		return sb.toString();
	}
	
	public List<DeltaResourceUsage> dumpAll(){
		List<DeltaResourceUsage> descList=new ArrayList<DeltaResourceUsage>();
		this.dumpAll(descList);
		return descList;
	}
	
	public void dumpAll(List<DeltaResourceUsage> descList){
		for(DeltaResourceUsage childDRU: childList){
			childDRU.dumpAll(descList);
			descList.add(childDRU);
		}
		descList.add(this);
	}
	
//	public CostMetrics getCostMetrics(){
//		CostMetrics cm=new CostMetrics();
//		computeCostMetricsRecursively(cm);
//		return cm;
//	}
	
//	public void computeCostMetricsRecursively(CostMetrics cm){
//		cm.addDeltaCPUTimeUS(this.deltaProcTimeUS);
//		cm.addDeltaMemoryBytes(this.deltaMemoryBytes);
//		for(DeltaResourceUsage childDRU: childList){
//			childDRU.computeCostMetricsRecursively(cm);
//			if(!this.getWorkerId().equals(childDRU.getWorkerId())){
//				cm.addDeltaOutputTimeUS(childDRU.deltaOutputTimeUS);
//			}
//		}
//	}
	
	public boolean containsInputRawStream(String workerId, RawStream rsl){
		Set<RawStream> rawSet=workerInputRawStreamsMap.get(workerId);
		if(rawSet!=null){
			if(rawSet.contains(rsl)){
				return true;
			}
		}
		return false;
	}
	
	public boolean containsInputDerivedStreamContainer(String workerId, DerivedStreamContainer psc){
		Set<DerivedStreamContainer> rawSet=workerInputContainersMap.get(workerId);
		if(rawSet!=null){
			if(rawSet.contains(psc)){
				return true;
			}
		}
		return false;
	}	
	
	public CandidateContainerType getType() {
		return type;
	}
	
	public void compute(){
		if(type==CandidateContainerType.ROOT_REUSE){
			this.computeRootReuse();
		}
		else if(type==CandidateContainerType.JOIN_DIRECT_REUSE){
			this.computeJoinReuse();
		}
		else if(type==CandidateContainerType.FILTER_DIRECT_REUSE){
			this.computeFilterReuse();
		}
		else if(type==CandidateContainerType.JOIN_INDIRECT_REUSE ||
				type==CandidateContainerType.FILTER_INDIRECT_REUSE){
			this.computeFilterOrJoinIndirectReuse();
		}
		else if(type==CandidateContainerType.ROOT_NEW){
			this.computeRootNew();
		}
		else if(type==CandidateContainerType.JOIN_NEW){
			this.computeJoinNew();
		}
		else if(type==CandidateContainerType.FILTER_NEW){
			this.computeFilterNew();
		}
	}
	
	public void compute(List<EventOrPropertySpecification> parentExtraCEPSList){
		this.parentExtraCEPSList=parentExtraCEPSList;
		compute();
	}
	public void computeFilterReuse(){
		deltaProcTimeUS = 0.0d;
		ereList=getExtraResultElementList(stream, container, eaMap);
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(ereList);
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS();
		deltaMemoryBytes = (double)deltaOutputBytesPerEvent * stream.getRate();
	}
	
	public void computeJoinReuse(){
		for(DeltaResourceUsage childDRU: childList){
			childDRU.compute();
		}
		deltaProcTimeUS=0.0;
		ereList=getExtraResultElementList(stream, container, eaMap);
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(ereList);		
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS();
		deltaMemoryBytes=0.0;
		for(DeltaResourceUsage childDRU: childList){
			double winTimeUS=childDRU.getContainer().getWindowTimeUS();
			deltaMemoryBytes += childDRU.getContainer().getRate() * winTimeUS * childDRU.deltaOutputBytesPerEvent;
		}
	}
	
	public void computeRootReuse(){
		DeltaResourceUsage childDRU=this.childList.get(0);
		childDRU.compute();
		
		deltaProcTimeUS = 0.0d;
		ereList=getExtraResultElementList(stream, container, eaMap);
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(ereList);
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS();
		deltaMemoryBytes = (double)deltaOutputBytesPerEvent * stream.getRate();
	}
	
	public void computeFilterOrJoinIndirectReuse(){
		if(containsInputDerivedStreamContainer(this.workerStat.id, container)){
			ereList=getExtraResultElementList(stream, container, eaMap);
		}
		else{
			ereList=stream.getResultElementList();
		}
		
		int comparisonCount=compResult.getImplyingAndSurplusComparisonExpressionCount();
		deltaProcTimeUS = (workerStat.filterCondProcTimeUS * comparisonCount * container.getRate());
		
		List<EventOrPropertySpecification> extraCEPSList=getExtraConditionEventOrSpecificationList(stream, compResult, eaMap);
		List<EventOrPropertySpecification> extraCEPSList2=eliminateDumplicated(extraCEPSList, stream.getResultElementList());
		
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(ereList);
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS() + workerStat.sendBaseTimeUS;
		double memoryBytesPerEvent = deltaOutputBytesPerEvent+
				sizeEstimator.computeEventOrPropertySpecificationsByteSize(extraCEPSList2);
		
		long outputIntervalUS = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS();
		deltaMemoryBytes = memoryBytesPerEvent * stream.getRate() * outputIntervalUS; //already consider localization, see above
		this.childList.get(0).compute(extraCEPSList2);
	}
	
	public void computeFilterNew(){
		ereList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(ereList);		
		int comparisonCount=((FilterStream)stream).getFilterExpr().getComparisonExpressionCount();
		deltaProcTimeUS = workerStat.filterCondProcTimeUS * comparisonCount * stream.getRate();
		deltaMemoryBytes=0.0;		
		RawStream rsl=((FilterStream)stream).getRawStream();
		if(!containsInputRawStream(this.getWorkerId(), rsl)){
			double bytes=sizeEstimator.computeSelectElementsByteSize(rsl.getResultElementList());
			long outputIntervalUS = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS();
			double rate=rawStats.getOutputRateSec(rsl);
			deltaMemoryBytes += rate * outputIntervalUS * bytes / 1e6;
		}
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS() + workerStat.sendBaseTimeUS;
	}
	
	public void computeJoinNew(){
		for(DeltaResourceUsage childDRU: childList){
			childDRU.compute();
		}
		
		ereList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(ereList);		
		int comparisonCount=AbstractBooleanExpression.getComparisonExpressionCount(((JoinStream)stream).getJoinExprList());
		
		//deltaProcTimeUS = workerStat.joinCondProcTimeUS * comparisonCount * location.getRate();		
		deltaMemoryBytes=0.0;
		long[] winTimeUS=new long[childList.size()];
		for(int i=0;i<childList.size();i++){//maybe local
			DeltaResourceUsage childDRU=childList.get(i);
			winTimeUS[i]=childDRU.getStream().getWindowTimeUS();
			if((childDRU.type==CandidateContainerType.FILTER_DIRECT_REUSE || childDRU.type==CandidateContainerType.JOIN_DIRECT_REUSE) &&
					containsInputDerivedStreamContainer(this.workerStat.id, childDRU.getContainer())){
				double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.ereList);
				deltaMemoryBytes += childDRU.container.getRate() * winTimeUS[i] * bytes / 1e6;
			}
			else{
				double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.getStream().getResultElementList());
				deltaMemoryBytes += childDRU.stream.getRate() * winTimeUS[i] * bytes / 1e6;
			}
		}
		deltaProcTimeUS = childList.get(0).stream.getRate() * 
						childList.get(1).stream.getRate() *
						comparisonCount *
						(winTimeUS[0] + winTimeUS[1]);
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS() + workerStat.sendBaseTimeUS;
	}
	
	public void computeRootNew(){
		DeltaResourceUsage childDRU=this.childList.get(0);
		childDRU.compute();
		
		ereList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(ereList);
		////deltaMemory = (double)deltaOutputByteSizePerEvent * location.getRate();
		
		long winTimeUS=childDRU.getStream().getWindowTimeUS();
		if((childDRU.type==CandidateContainerType.FILTER_DIRECT_REUSE || childDRU.type==CandidateContainerType.JOIN_DIRECT_REUSE) &&
				containsInputDerivedStreamContainer(this.workerStat.id, childDRU.getContainer())){
			double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.ereList);
			deltaMemoryBytes += childDRU.container.getRate() * winTimeUS * bytes / 1e6;
		}
		else{
			double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.getStream().getResultElementList());
			deltaMemoryBytes += childDRU.getStream().getRate() * winTimeUS * bytes / 1e6;
		}
		int comparisonCount=AbstractBooleanExpression.getComparisonExpressionCount(((RootStream)stream).getWhereExprList());
		
		deltaProcTimeUS = workerStat.filterCondProcTimeUS * comparisonCount * stream.getRate();		
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS() + workerStat.sendBaseTimeUS;
	}
	
	public static List<EventOrPropertySpecification> eliminateDumplicated(
			List<EventOrPropertySpecification> epsList,
			List<SelectClauseExpressionElement> seList){
		List<EventOrPropertySpecification> extraEpsList=new ArrayList<EventOrPropertySpecification>(epsList.size());
		for(EventOrPropertySpecification eps: epsList){
			boolean isAppeared=false;
			for(SelectClauseExpressionElement se: seList){
				EventOrPropertySpecification sceps=(EventOrPropertySpecification)se.getSelectExpr();
				EPSRelation r=epsComparator.compare(eps, sceps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					isAppeared=true;
					break;
				}
			}
			if(!isAppeared){
				extraEpsList.add(eps);
			}
		}
		return extraEpsList;
	}
	
	public static List<EventOrPropertySpecification> eliminateDumplicated2(
			Collection<EventOrPropertySpecification> epsList1,
			Collection<EventOrPropertySpecification> epsList2,
			Map<EventAlias,EventAlias> eaMap, 
			List<EventOrPropertySpecification> extraEpsList){
		//List<EventOrPropertySpecification> extraEpsList=new ArrayList<EventOrPropertySpecification>(epsList1.size());
		epsComparator.setCompareStrategy(CompareStrategy.REPLACE_EVENTALIAS_MATCH);
		epsComparator.setEventAliasMap(eaMap);
		for(EventOrPropertySpecification eps1: epsList1){
			boolean isAppeared=false;
			for(EventOrPropertySpecification eps2: epsList2){
				EPSRelation r=epsComparator.compare(eps1, eps2);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){//eps1 is contained by eps2
					isAppeared=true;
					break;
				}
			}
			if(!isAppeared){
				extraEpsList.add(eps1);
			}
		}
		return extraEpsList;
	}
	
	public static List<EventOrPropertySpecification> getExtraConditionEventOrSpecificationList(
			DerivedStream psl,
			BooleanExpressionComparisonResult compResult,
			Map<EventAlias,EventAlias> eaMap){
		List<EventOrPropertySpecification> extraEpsList=new ArrayList<EventOrPropertySpecification>();
		Set<EventOrPropertySpecification> epsSet1=new HashSet<EventOrPropertySpecification>();
		Set<EventOrPropertySpecification> epsSet2=new HashSet<EventOrPropertySpecification>();
		epsComparator.setCompareStrategy(CompareStrategy.REPLACE_EVENTALIAS_MATCH);
		epsComparator.setEventAliasMap(eaMap);
		for(BooleanExpressionComparisonPair pair: compResult.getOwnPairList()){
			if(pair.getState()==State.IMPLYING || 
				pair.getState()==State.SURPLUS){
				epsSet1.clear();
				epsSet2.clear();
				/*pair.getFirst().dumpAllEventOrPropertySpecReferences(epsSet1);*/
				EventOrPropertySpecReferenceDumper.dump(pair.getFirst(), epsSet1);
				if(pair.getSecond()!=null){
					/*pair.getSecond().dumpAllEventOrPropertySpecReferences(epsSet2);*/
					EventOrPropertySpecReferenceDumper.dump(pair.getSecond(), epsSet2);
				}
				eliminateDumplicated2(epsSet1, epsSet2, eaMap, extraEpsList);
			}
		}
		for(BooleanExpressionComparisonPair pair: compResult.getChildPairList()){
			if(pair.getState()==State.IMPLYING || 
				pair.getState()==State.SURPLUS){
				epsSet1.clear();
				epsSet2.clear();
				/*pair.getFirst().dumpAllEventOrPropertySpecReferences(epsSet1);*/
				EventOrPropertySpecReferenceDumper.dump(pair.getFirst(), epsSet1);
				if(pair.getSecond()!=null){
					/*pair.getSecond().dumpAllEventOrPropertySpecReferences(epsSet2);*/
					EventOrPropertySpecReferenceDumper.dump(pair.getSecond(), epsSet2);
				}
				eliminateDumplicated2(epsSet1, epsSet2, eaMap, extraEpsList);
			}
		}
		return extraEpsList;
	}
	
	public static List<SelectClauseExpressionElement> getExtraResultElementList(
			DerivedStream psl, 
			DerivedStreamContainer psc,
			Map<EventAlias,EventAlias> eaMap){//FIXME: use eaMap
		List<SelectClauseExpressionElement> ereList=new ArrayList<SelectClauseExpressionElement>(psl.getResultElementList().size());
		epsComparator.setCompareStrategy(CompareStrategy.REPLACE_EVENTALIAS_MATCH);
		epsComparator.setEventAliasMap(eaMap);
		for(SelectClauseExpressionElement slse: psl.getResultElementList()){
			EventOrPropertySpecification sleps=(EventOrPropertySpecification)slse.getSelectExpr();
			boolean isAppeared=false;
			for(SelectClauseExpressionElement scse: psc.getResultElementList()){
				EventOrPropertySpecification sceps=(EventOrPropertySpecification)scse.getSelectExpr();
				EPSRelation r=epsComparator.compare(sleps, sceps);
				if(r==EPSRelation.EQUAL || r==EPSRelation.IS_CONTAINED){
					isAppeared=true;
					break;
				}
			}
			if(!isAppeared){
				ereList.add(slse);
			}
		}
		return ereList;
	}
	
	public DeltaResourceUsage(
			WorkerStat workerStat,
			CandidateContainerType type,
			DerivedStream psl,
			DerivedStreamContainer psc) {
		this(workerStat, type, psl, psc, null, null);
	}
	
	public DeltaResourceUsage(
			WorkerStat workerStat,
			CandidateContainerType type,
			DerivedStream psl,
			DerivedStreamContainer psc,
			Map<EventAlias,EventAlias> eaMap,
			BooleanExpressionComparisonResult compResult) {
		super();
		this.workerStat = workerStat;		
		this.type = type;
		this.stream = psl;
		this.container = psc;
		this.eaMap = eaMap;
		this.compResult = compResult;
	}

	public DeltaResourceUsage(
			WorkerStat workerStat,
			CandidateContainerType type,
			DerivedStream psl) {
		this(workerStat, type, psl, null);
	}
	
	public void addChild(DeltaResourceUsage dru){
		childList.add(dru);
	}
	
	public List<DeltaResourceUsage> getChildList(){
		return this.childList;
	}
	
	public DeltaResourceUsage getChild(int index){
		return this.childList.get(index);
	}
	
	public int getChildCount(){
		return this.childList.size();
	}
	
	public void reset(){
		this.parentExtraCEPSList=Collections.emptyList();
		childList.clear();
	}

	public DerivedStreamContainer getContainer() {
		return container;
	}

	public void setContainer(DerivedStreamContainer container) {
		this.container = container;
	}
	
	public DerivedStream getStream() {
		return stream;
	}

	public void setStream(DerivedStream stream) {
		this.stream = stream;
	}	

	public static Map<String, Set<DerivedStreamContainer>> getWorkerInputContainersMap() {
		return workerInputContainersMap;
	}

	public static void setWorkerInputContainersMap(
			MultiValueMap<String, DerivedStreamContainer> workerInputContainersMap) {
		DeltaResourceUsage.workerInputContainersMap = workerInputContainersMap;
	}

	public static Map<String, Set<RawStream>> getWorkerInputRawStreamsMap() {
		return workerInputRawStreamsMap;
	}

	public static void setWorkerInputRawStreamsMap(
			MultiValueMap<String, RawStream> workerInputRawStreamsMap) {
		DeltaResourceUsage.workerInputRawStreamsMap = workerInputRawStreamsMap;
	}

	public static RawStats getRawStat() {
		return rawStats;
	}

	public static void setRawStat(RawStats rawStat) {
		DeltaResourceUsage.rawStats = rawStat;
	}

	public Map<EventAlias, EventAlias> getEaMap() {
		return eaMap;
	}

	public void setEventAliasMap(Map<EventAlias, EventAlias> eaMap) {
		this.eaMap = eaMap;
	}

	public BooleanExpressionComparisonResult getCompResult() {
		return compResult;
	}

	public void setCompResult(BooleanExpressionComparisonResult compResult) {
		this.compResult = compResult;
	}

	public static EventOrPropertySizeEstimator getSizeEstimator() {
		return sizeEstimator;
	}

	public static void setSizeEstimator(EventOrPropertySizeEstimator sizeEstimator) {
		DeltaResourceUsage.sizeEstimator = sizeEstimator;
	}
	
	public static enum CandidateContainerType{
		FILTER_NEW("filter_new"),
		FILTER_DIRECT_REUSE("filter_direct_reuse"),		
		FILTER_INDIRECT_REUSE("filter_indirect_reuse"),
		JOIN_NEW("join_new"),
		JOIN_DIRECT_REUSE("join_direct_reuse"),
		JOIN_INDIRECT_REUSE("join_indirect_reuse"),
		PATTERN("pattern"),
		ROOT_NEW("root_new"),
		ROOT_REUSE("root_reuse");
		
		String str;
		CandidateContainerType(String str){
			this.str=str;
		}
		@Override
		public String toString(){
			return str;
		}
	}
}


