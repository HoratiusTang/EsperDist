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
	public DerivedStream stream;//if type is XXX_INDIRECT_REUSE, stream is own stream
	public DerivedStreamContainer container;//null if new; if type is XXX_INDIRECT_REUSE, container is agent's container
	public InstanceStat containerStat;//container's stat
	public Map<EventAlias, EventAlias> eaMap;
	public BooleanExpressionComparisonResult compResult;//only compatible
	//public double outputRate;//count per ms
	//public long outputIntervalUS=2000000;//us
	
	public double deltaMemoryBytes;//bytes=sum(child.deltaOutputBytesSize)
	public double deltaProcTimeUS;//us per s
	private double deltaOutputBytesPerEvent;
	//private double deltaMemoryBytesPerEvent;
	public double deltaOutputTimeUS;//us per s
	public List<SelectClauseExpressionElement> extraOutputElementList;
	public List<SelectClauseExpressionElement> extraInputElementList;
	List<EventOrPropertySpecification> parentExtraConditionEPSList=Collections.emptyList();
	public List<DeltaResourceUsage> childList=new ArrayList<DeltaResourceUsage>(2);
	
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
	
	public boolean containsInputRawStream(String workerId, RawStream rsl){
		Set<RawStream> rawSet=workerInputRawStreamsMap.get(workerId);
		if(rawSet!=null && rawSet.contains(rsl)){
			return true;			
		}
		return false;
	}
	
	public boolean containsInputDerivedStreamContainer(String workerId, DerivedStreamContainer dsc){
		if(workerId.equals(dsc.getWorkerId().getId())){
			return true;//it's own container
		}
		Set<DerivedStreamContainer> dscSet=workerInputContainersMap.get(workerId);
		if(dscSet!=null && dscSet.contains(dsc)){
			return true;
		}
		return false;
	}	
	
	public CandidateContainerType getType() {
		return type;
	}
	
	public void compute(String parentWorkerId){
		if(type==CandidateContainerType.ROOT_REUSE){
			this.computeRootReuse();
		}
		else if(type==CandidateContainerType.JOIN_DIRECT_REUSE){
			this.computeJoinReuse(parentWorkerId);
		}
		else if(type==CandidateContainerType.JOIN_INDIRECT_REUSE){
			this.computeJoinIndirectReuse(parentWorkerId);
		}
		else if(type==CandidateContainerType.FILTER_DIRECT_REUSE){
			this.computeFilterReuse(parentWorkerId);
		}
		else if(type==CandidateContainerType.FILTER_INDIRECT_REUSE){
			this.computeFilterIndirectReuse(parentWorkerId);
		}
		else if(type==CandidateContainerType.ROOT_NEW){
			this.computeRootNew(parentWorkerId);
		}
		else if(type==CandidateContainerType.JOIN_NEW){
			this.computeJoinNew(parentWorkerId);
		}
		else if(type==CandidateContainerType.FILTER_NEW){
			this.computeFilterNew(parentWorkerId);
		}
	}
	
	private int additionalMemorySizeWhenNewOutputPerEvent(int numSelectElement){
		return 10*1000+numSelectElement*400;
	}
	
	private int additionalMemorySizeWhenReuseOutputPerEvent(int numSelectElement){
		return numSelectElement*400;
	}
	
	public void compute(String parentWorkerId, List<EventOrPropertySpecification> parentExtraConditionEPSList){
		this.parentExtraConditionEPSList=parentExtraConditionEPSList;
		compute(parentWorkerId);
	}
	public void computeFilterReuse(String parentWorkerId){//FIXME: ignore parentExtraConditionEPSList ahora
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		deltaProcTimeUS = 0.0d;
		extraOutputElementList=getExtraResultElementList(stream, container, eaMap);
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		if(this.getWorkerId().equals(parentWorkerId)){//send in memory
			deltaOutputTimeUS = 0;
			deltaMemoryBytes = additionalMemorySizeWhenReuseOutputPerEvent(extraOutputElementList.size()) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		else{//send via socket
			deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
								+ workerStat.getSendBaseTimeUS();
			deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenReuseOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
	}
	
	public void computeJoinReuse(String parentWorkerId){//FIXME: ignore parentExtraConditionEPSList ahora
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		for(DeltaResourceUsage childDRU: childList){
			childDRU.compute(this.getWorkerId());
		}
		deltaProcTimeUS=0.0;
		extraOutputElementList=getExtraResultElementList(stream, container, eaMap);
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		if(this.getWorkerId().equals(parentWorkerId)){//send in memory
			deltaOutputTimeUS = 0;
			deltaMemoryBytes = additionalMemorySizeWhenReuseOutputPerEvent(extraOutputElementList.size()) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		else{
			deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
								+ workerStat.getSendBaseTimeUS();
			deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenReuseOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		for(DeltaResourceUsage childDRU: childList){
			double winTimeUS=childDRU.getContainer().getWindowTimeUS();
			deltaMemoryBytes += childDRU.getContainer().getRate() * winTimeUS * childDRU.deltaOutputBytesPerEvent;
		}
	}
	
	public void computeRootReuse(){
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		DeltaResourceUsage childDRU=this.childList.get(0);
		childDRU.compute(this.getWorkerId());
		
		deltaProcTimeUS = 0.0d;
		extraOutputElementList=getExtraResultElementList(stream, container, eaMap);
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		//assume send to new target 
		deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
							+ workerStat.getSendBaseTimeUS();
		
		double winTimeUS=childDRU.getContainer().getWindowTimeUS();
		deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenReuseOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
							+ type.getFixedDeltaMemoryBytes()
							+ childDRU.getContainer().getRate() * winTimeUS * childDRU.deltaOutputBytesPerEvent;
	}
	
	public void computeFilterNew(String parentWorkerId){
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		extraOutputElementList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);		
		
		if(this.getWorkerId().equals(parentWorkerId)){//send in memory
			deltaOutputTimeUS = 0.0;
			deltaMemoryBytes = additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size()) * stream.getRate() * outputIntervalSec 
							+ type.getFixedDeltaMemoryBytes();
		}
		else{
			deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
								+ workerStat.getSendBaseTimeUS();
			deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		RawStream rsl=((FilterStream)stream).getRawStream();
		double rsRate=rawStats.getOutputRateSec(rsl);
		int comparisonCount=((FilterStream)stream).getFilterExpr().getComparisonExpressionCount();
		deltaProcTimeUS = (workerStat.filterCondProcTimeUS * comparisonCount + type.getFixedDeltaProcTimeUsPerEvent()) * rsRate * 1.0 ;
		if(!containsInputRawStream(this.getWorkerId(), rsl)){
			/*RawStream is new to this worker*/
			double bytes=sizeEstimator.computeSelectElementsByteSize(rsl.getResultElementList());
			deltaMemoryBytes += bytes * rsRate * outputIntervalSec;
		}		
	}
	
	public void computeJoinNew(String parentWorkerId){
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		for(DeltaResourceUsage childDRU: childList){
			childDRU.compute(this.getWorkerId());
		}		
		extraOutputElementList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		int comparisonCount=AbstractBooleanExpression.getComparisonExpressionCount(((JoinStream)stream).getJoinExprList());
		if(this.getWorkerId().equals(parentWorkerId)){//send in memory
			deltaOutputTimeUS = 0.0d;
			deltaMemoryBytes = additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size()) * stream.getRate() * outputIntervalSec
					+ type.getFixedDeltaMemoryBytes();
		}
		else{
			deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
								+ workerStat.getSendBaseTimeUS();
			deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		long[] winTimeUS=new long[childList.size()];
		double[] rateSec=new double[childList.size()];
		for(int i=0;i<childList.size();i++){//maybe local
			DeltaResourceUsage childDRU=childList.get(i);
			winTimeUS[i]=childDRU.getStream().getWindowTimeUS();
			if((childDRU.type==CandidateContainerType.FILTER_DIRECT_REUSE || childDRU.type==CandidateContainerType.JOIN_DIRECT_REUSE) &&
					containsInputDerivedStreamContainer(this.workerStat.id, childDRU.getContainer())){
				double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.extraOutputElementList);
				rateSec[i] = childDRU.containerStat.getOutputRateSec();
				deltaMemoryBytes += bytes * rateSec[i] * winTimeUS[i] / 1e6;				
			}
			else{
				double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.getStream().getResultElementList());
				rateSec[i] = childDRU.stream.getRate();
				deltaMemoryBytes += bytes * rateSec[i] * winTimeUS[i] / 1e6;
			}
		}
		deltaProcTimeUS = (workerStat.filterCondProcTimeUS * comparisonCount + type.getFixedDeltaProcTimeUsPerEvent())* 
						rateSec[0] * rateSec[1] * (winTimeUS[0] + winTimeUS[1]) / 1e6;
	}
	
	public void computeRootNew(String parentWorkerId){
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		DeltaResourceUsage childDRU=this.childList.get(0);
		childDRU.compute(this.getWorkerId());
		
		extraOutputElementList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		
		deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
							+ workerStat.getSendBaseTimeUS();
		deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenReuseOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
							+ type.getFixedDeltaMemoryBytes();

		long winTimeUS=childDRU.getStream().getWindowTimeUS();
		double rateSec;
		if((childDRU.type==CandidateContainerType.FILTER_DIRECT_REUSE || childDRU.type==CandidateContainerType.JOIN_DIRECT_REUSE) &&
				containsInputDerivedStreamContainer(this.workerStat.id, childDRU.getContainer())){
			double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.extraOutputElementList);
			rateSec=childDRU.containerStat.getOutputRateSec();
			deltaMemoryBytes += bytes * rateSec * winTimeUS / 1e6;
		}
		else{
			double bytes=sizeEstimator.computeSelectElementsByteSize(childDRU.getStream().getResultElementList());
			rateSec=childDRU.stream.getRate();
			deltaMemoryBytes += bytes * rateSec * winTimeUS / 1e6;
		}
		int comparisonCount=AbstractBooleanExpression.getComparisonExpressionCount(((RootStream)stream).getWhereExprList());
		deltaProcTimeUS = (workerStat.filterCondProcTimeUS * comparisonCount + type.getFixedDeltaProcTimeUsPerEvent()) * childDRU.stream.getRate() * 1.0;
	}
	
	public void computeFilterIndirectReuse(String parentWorkerId){
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		/* stream is own stream, container is agent's container */		
		extraOutputElementList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		int comparisonCount=compResult.getImplyingAndSurplusComparisonExpressionCount();
		deltaProcTimeUS = (workerStat.filterCondProcTimeUS * comparisonCount + type.getFixedDeltaProcTimeUsPerEvent()) * stream.getRate() * 1.0;
		
		/* this -> downstream */
		if(this.getWorkerId().equals(parentWorkerId)){//send in memory
			deltaOutputTimeUS = 0.0d;
			deltaMemoryBytes = additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size()) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		else{
			deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
								+ workerStat.getSendBaseTimeUS();
			deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		
		/* upstream -> this */
		if(containsInputDerivedStreamContainer(this.workerStat.id, container))
			extraInputElementList=getExtraResultElementList(stream, container, eaMap);
		else
			extraInputElementList=stream.getResultElementList();

		List<EventOrPropertySpecification> extraCEPSList2=getExtraConditionEventOrSpecificationListExceptSelectElements(stream, compResult, eaMap);
		
		deltaMemoryBytes += (sizeEstimator.computeSelectElementsByteSize(extraInputElementList) 
				+ sizeEstimator.computeEventOrPropertySpecificationsByteSize(extraCEPSList2)) * stream.getRate() * outputIntervalSec; //already consider localization, see above
		this.childList.get(0).compute(this.getWorkerId(), extraCEPSList2);
	}
	
	public void computeJoinIndirectReuse(String parentWorkerId){
		double outputIntervalSec = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS()/1e6;
		/* stream is own stream, container is agent's container */
		extraOutputElementList=stream.getResultElementList();
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		int comparisonCount=compResult.getImplyingAndSurplusComparisonExpressionCount();
		deltaProcTimeUS = (workerStat.filterCondProcTimeUS * comparisonCount + type.getFixedDeltaProcTimeUsPerEvent())* stream.getRate();
		
		/* this -> downstream */
		if(this.getWorkerId().equals(parentWorkerId)){//send in memory
			deltaOutputTimeUS = 0.0d;
			deltaMemoryBytes = additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size()) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		else{
			deltaOutputTimeUS = deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS()
								+ workerStat.getSendBaseTimeUS();
			deltaMemoryBytes = (deltaOutputBytesPerEvent*2 + additionalMemorySizeWhenNewOutputPerEvent(extraOutputElementList.size())) * stream.getRate() * outputIntervalSec
								+ type.getFixedDeltaMemoryBytes();
		}
		
		/* upstream -> this */
		if(containsInputDerivedStreamContainer(this.workerStat.id, container))
			extraInputElementList=getExtraResultElementList(stream, container, eaMap);
		else
			extraInputElementList=stream.getResultElementList();

		List<EventOrPropertySpecification> extraCEPSList2=getExtraConditionEventOrSpecificationListExceptSelectElements(stream, compResult, eaMap);
		
		deltaMemoryBytes += (sizeEstimator.computeSelectElementsByteSize(extraInputElementList) 
				+ sizeEstimator.computeEventOrPropertySpecificationsByteSize(extraCEPSList2)) * stream.getRate() * outputIntervalSec; //already consider localization, see above
		for(DeltaResourceUsage childDRU: this.childList){
			childDRU.compute(this.getWorkerId(), extraCEPSList2);
		}
	}
	
	/**
	public void computeJoinIndirectReuse0(String parentWorkerId){		
		if(containsInputDerivedStreamContainer(this.workerStat.id, container)){
			extraOutputElementList=getExtraResultElementList(stream, container, eaMap);
		}
		else{
			extraOutputElementList=stream.getResultElementList();
		}
		
		int comparisonCount=compResult.getImplyingAndSurplusComparisonExpressionCount();
		deltaProcTimeUS = (workerStat.filterCondProcTimeUS * comparisonCount * container.getRate());
		
		List<EventOrPropertySpecification> extraCEPSList=getExtraConditionEventOrSpecificationList(stream, compResult, eaMap);
		List<EventOrPropertySpecification> extraCEPSList2=eliminateDumplicated(extraCEPSList, stream.getResultElementList());
		
		deltaOutputBytesPerEvent = sizeEstimator.computeSelectElementsByteSize(extraOutputElementList);
		deltaOutputTimeUS = (double)deltaOutputBytesPerEvent * stream.getRate() / workerStat.getSendByteRateUS() + workerStat.sendBaseTimeUS;
		double memoryBytesPerEvent = deltaOutputBytesPerEvent+
				sizeEstimator.computeEventOrPropertySpecificationsByteSize(extraCEPSList2);
		
		long outputIntervalUS = ServiceManager.getInstance(this.getWorkerId()).getOutputIntervalUS();
		deltaMemoryBytes = memoryBytesPerEvent * stream.getRate() * outputIntervalUS; //already consider localization, see above
		this.childList.get(0).compute(this.getWorkerId(), extraCEPSList2);		
	}*/
	
	public static List<EventOrPropertySpecification> getExtraConditionEventOrSpecificationListExceptSelectElements(
		DerivedStream ds,
		BooleanExpressionComparisonResult compResult,
		Map<EventAlias,EventAlias> eaMap){
		List<EventOrPropertySpecification> extraCEPSList=getExtraConditionEventOrSpecificationList(ds, compResult, eaMap);
		List<EventOrPropertySpecification> extraCEPSList2=eliminateDumplicated(extraCEPSList, ds.getResultElementList());
		return extraCEPSList2; 
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
			DerivedStreamContainer psc,
			InstanceStat instanceStat,
			Map<EventAlias,EventAlias> eaMap,
			BooleanExpressionComparisonResult compResult) {
		super();
		this.workerStat = workerStat;		
		this.type = type;
		this.stream = psl;
		this.container = psc;
		this.containerStat = instanceStat;
		this.eaMap = eaMap;
		this.compResult = compResult;
	}
	
	public DeltaResourceUsage(
			WorkerStat workerStat,
			CandidateContainerType type,
			DerivedStream psl,
			DerivedStreamContainer psc, 
			InstanceStat instanceStat) {
		this(workerStat, type, psl, psc, null, null, null);
	}

	public DeltaResourceUsage(
			WorkerStat workerStat,
			CandidateContainerType type,
			DerivedStream psl) {
		this(workerStat, type, psl, null, null);
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
		this.parentExtraConditionEPSList=Collections.emptyList();
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

	public InstanceStat getContainerStat() {
		return containerStat;
	}

	public void setContainerStat(InstanceStat containerStat) {
		this.containerStat = containerStat;
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
		FILTER_NEW("filter_new", 100*1000, 2),
		FILTER_DIRECT_REUSE("filter_direct_reuse", 5*1000, 0),
		FILTER_INDIRECT_REUSE("filter_indirect_reuse", 100*1000, 2),
		JOIN_NEW("join_new", 500*1000, 5),
		JOIN_DIRECT_REUSE("join_direct_reuse", 8*1000, 0),
		JOIN_INDIRECT_REUSE("join_indirect_reuse", 100*1000, 2),
		PATTERN("pattern", 1000*1000, 10),
		ROOT_NEW("root_new", 100*1000, 2),
		ROOT_REUSE("root_reuse", 5*1000, 0);
		
		String str;
		int fixedDeltaMemoryBytes;//bytes
		int fixedDeltaProcTimeUsPerEvent;
		CandidateContainerType(String str, int fixedDeltaMemoryBytes, int fixedDeltaProcTimeUsPerEvent){
			this.str = str;
			this.fixedDeltaMemoryBytes = fixedDeltaMemoryBytes;
			this.fixedDeltaProcTimeUsPerEvent = fixedDeltaProcTimeUsPerEvent;
		}
		@Override
		public String toString(){
			return str;
		}
		public int getFixedDeltaMemoryBytes() {
			return fixedDeltaMemoryBytes;
		}
		public void setFixedDeltaMemoryBytes(int fixedDeltaMemoryBytes) {
			this.fixedDeltaMemoryBytes = fixedDeltaMemoryBytes;
		}
		public int getFixedDeltaProcTimeUsPerEvent() {
			return fixedDeltaProcTimeUsPerEvent;
		}
		public void setFixedDeltaProcTimeUsPerEvent(int fixedDeltaProcTimeUsPerEvent) {
			this.fixedDeltaProcTimeUsPerEvent = fixedDeltaProcTimeUsPerEvent;
		}
	}
}


