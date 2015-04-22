package dist.esper.core.flow.container;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dist.esper.core.flow.stream.DerivedStream;
import dist.esper.core.flow.stream.JoinStream;
import dist.esper.core.flow.stream.Stream;
import dist.esper.core.id.WorkerId;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.DeepCloneReplaceFactory;
import dist.esper.epl.expr.util.DeepReplaceFactory;
import dist.esper.epl.expr.util.ExpressionComparator;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;
import dist.esper.epl.expr.util.ExpressionComparator.CompareStrategy;
import dist.esper.util.CollectionUtils;
import dist.esper.util.ITuple3D;
import dist.esper.util.Tuple2D;
import dist.esper.util.Tuple3D;

/**
 * the super-class of all concrete StreamContainers, 
 * NOTE there is no RawStreamContainer but only @RawStream.
 * 
 * @see @DerivedStream
 * @author tjy
 *
 */
public abstract class DerivedStreamContainer extends StreamContainer {	
	private static final long serialVersionUID = 2060269911835504916L;
	List<StreamAndMapAndBoolComparisonResult> directReuseStreamMapComparisonResultList=new ArrayList<StreamAndMapAndBoolComparisonResult>(4);
	List<StreamAndMapAndBoolComparisonResult> indirectReuseStreamMapComparisonResultList=new ArrayList<StreamAndMapAndBoolComparisonResult>(4);
	
	public transient static ExpressionComparator exprComp=new ExpressionComparator();

	public static final long UNCONCERNED_WINDOW_TIME=-1;
	long windowTimeUS=UNCONCERNED_WINDOW_TIME;
	public abstract ViewSpecification[] getViewSpecs();
	public abstract Stream[] getUpStreams();
	public abstract AbstractBooleanExpression getOwnCondition();	
	
	public String getWindowTimeViewSpecString(){
		return String.format(".win:time(%d msec)", this.getWindowTimeUS()/1000);
	}
	
	public long getWindowTimeUS() {
		return windowTimeUS;
	}

	public void setWindowTimeUS(long windowTimeUS) {
		this.windowTimeUS = windowTimeUS;
	}	

	public void addDirectReuseStreamMapComparisonResult(StreamAndMapAndBoolComparisonResult lmcr){
		this.directReuseStreamMapComparisonResultList.add(lmcr);
	}
	
	public void addIndirectReuseStreamMapComparisonResult(StreamAndMapAndBoolComparisonResult lmcr){
		this.indirectReuseStreamMapComparisonResultList.add(lmcr);
	}
	
	public void setDirectReuseStreamMapComparisonResultList(
			List<StreamAndMapAndBoolComparisonResult> directReuseStreamMapComparisonResultList) {
		this.directReuseStreamMapComparisonResultList = directReuseStreamMapComparisonResultList;
	}

	public void setIndirectReuseStreamMapComparisonResultList(
			List<StreamAndMapAndBoolComparisonResult> indirectReuseStreamMapComparisonResultList) {
		this.indirectReuseStreamMapComparisonResultList = indirectReuseStreamMapComparisonResultList;
	}

	public List<StreamAndMapAndBoolComparisonResult> getDirectReuseStreamMapComparisonResultList() {
		return directReuseStreamMapComparisonResultList;
	}

	public List<StreamAndMapAndBoolComparisonResult> getIndirectReuseStreamMapComparisonResultList() {
		return indirectReuseStreamMapComparisonResultList;
	}

	public StreamAndMapAndBoolComparisonResult getDirectReuseStreamMapComparisonResultByEplId(long eplId){
		for(StreamAndMapAndBoolComparisonResult lmc: this.directReuseStreamMapComparisonResultList){
			if(lmc.getFirst().getEplId()==eplId){
				return lmc;
			}
		}
		return null;
	}
	public StreamAndMapAndBoolComparisonResult getIndirectReuseStreamMapComparisonResultByEplId(long eplId){
		for(StreamAndMapAndBoolComparisonResult lmc: this.indirectReuseStreamMapComparisonResultList){
			if(lmc.getFirst().getEplId()==eplId){
				return lmc;
			}
		}
		return null;
	}
	public StreamAndMapAndBoolComparisonResult getDirectOrIndirectReuseStreamMapComparisonResultByEplId(long eplId){
		StreamAndMapAndBoolComparisonResult lmc=getDirectReuseStreamMapComparisonResultByEplId(eplId);
		if(lmc==null){
			lmc=getIndirectReuseStreamMapComparisonResultByEplId(eplId);
		}
		return lmc;
	}
	
	public List<Long> getDirectReuseStreamMapComparisonResultEplIdList(){
		return getLMCREplIdList(this.directReuseStreamMapComparisonResultList);
	}
	public List<Long> getIndirectReuseStreamMapComparisonResultEplIdList(){
		return getLMCREplIdList(this.indirectReuseStreamMapComparisonResultList);
	}
	private static List<Long> getLMCREplIdList(List<StreamAndMapAndBoolComparisonResult> lmcrList){
		List<Long> idList=new ArrayList<Long>(lmcrList.size());
		for(StreamAndMapAndBoolComparisonResult lmcr: lmcrList){
			idList.add(lmcr.getFirst().getEplId());
		}
		return idList;
	}
	public boolean isNew(){
		//return this.streamLocationList.size()<=1;
		return this.directReuseStreamMapComparisonResultList.size()+this.indirectReuseStreamMapComparisonResultList.size()<=1;
	}
	public void setUniqueName(String uniqueName) {
		super.setUniqueName(uniqueName);
		for(StreamAndMapAndBoolComparisonResult lmcr: this.directReuseStreamMapComparisonResultList){
			lmcr.getFirst().setUniqueName(uniqueName);
		}
	}
	public void setWorkerId(WorkerId workerId) {
		super.setWorkerId(workerId);
		for(StreamAndMapAndBoolComparisonResult lmcr: this.directReuseStreamMapComparisonResultList){
			lmcr.getFirst().setWorkerId(workerId);
		}
	}
	
	public abstract Stream getUpStreamByEventName(String childEventName);
	public void merge(DerivedStream pcsl, 
			Map<EventAlias,EventAlias> scToSlMap, 
			BooleanExpressionComparisonResult cr){
		this.setEplId(pcsl.getEplId());
		DeepReplaceFactory replacer=new DeepReplaceFactory(scToSlMap);
		mergeResultList(pcsl, replacer);
		mergeEventAliasMap(scToSlMap);
		Map<EventAlias, EventAlias> selfMap=CollectionUtils.makeMap(scToSlMap.values());//to itself
		if(cr.getTotalState()==State.EQUIVALENT){
			mergeMeta(pcsl);
			this.directReuseStreamMapComparisonResultList.add(
					new StreamAndMapAndBoolComparisonResult(pcsl, selfMap, cr));
		}
		else if(cr.getTotalState()==State.IMPLYING){
			this.indirectReuseStreamMapComparisonResultList.add(
					new StreamAndMapAndBoolComparisonResult(pcsl, selfMap, cr));
		}
	}
		
	protected void mergeMeta(DerivedStream pcsl){
		pcsl.setUniqueName(this.getUniqueName());
		pcsl.setWorkerId(this.getWorkerId());
	}
	
	/**
	 * merge {oldStream->container} with current {container->newStream}, the newStream will become contain's EventAlias latter
	 * @param containerToNewStreamMap
	 */
	public void mergeEventAliasMap(Map<EventAlias, EventAlias> containerToNewStreamMap){
		for(StreamAndMapAndBoolComparisonResult oldStreamToContainerSMCR: this.directReuseStreamMapComparisonResultList){
			Map<EventAlias, EventAlias> map2=CollectionUtils.merge(oldStreamToContainerSMCR.getSecond(), containerToNewStreamMap);
			oldStreamToContainerSMCR.setSecond(map2);
		}
		for(StreamAndMapAndBoolComparisonResult oldStreamToContainerSMCR: this.indirectReuseStreamMapComparisonResultList){
			Map<EventAlias, EventAlias> map2=CollectionUtils.merge(oldStreamToContainerSMCR.getSecond(), containerToNewStreamMap);
			oldStreamToContainerSMCR.setSecond(map2);
		}
	}
	public DerivedStreamContainer mergeResultList(DerivedStream pcsl, DeepReplaceFactory replacer){
		boolean[] pcslElementExisted=new boolean[pcsl.getResultElementList().size()];
		Arrays.fill(pcslElementExisted, false);
		
		for(SelectClauseExpressionElement thisElement: this.getResultElementList()){
			replacer.deepReplace((SelectExpressionElementContainer)thisElement);//SelectExpressionElementContainer
		}
		
		//wrong: exprComp.setEventAliasMap(replacer.getEventAliasReplaceMap());
		exprComp.setCompareStrategy(CompareStrategy.EVENTALIAS_MATCH);//already replaced
		for(int i=0; i<pcsl.getResultElementList().size(); i++){
			SelectClauseExpressionElement pcslElement=pcsl.getResultElementList().get(i);
			for(SelectClauseExpressionElement thisElement: this.getResultElementList()){
				if(exprComp.compare(pcslElement.getSelectExpr(), thisElement.getSelectExpr())){//FIXME
					pcslElement.setAssigndName(thisElement.getAssigndName());
					pcslElement.setUniqueName(thisElement.getUniqueName());
					pcslElementExisted[i]=true;
					break;
				}
			}
		}
		for(int i=0; i<pcsl.getResultElementList().size(); i++){
			if(!pcslElementExisted[i]){
				this.addResultElement(new SelectExpressionElementContainer(pcsl.getResultElementList().get(i)));
			}
		}
		return this;
	}
	
	public abstract void dumpEventAlias(Set<EventAlias> eaSet);
	public Set<EventAlias> dumpEventAlias(){
		Set<EventAlias> eaSet=new HashSet<EventAlias>();
		dumpEventAlias(eaSet);
		return eaSet;
	}
	
	public abstract void dumpChildrenBooleanExpressions(List<AbstractBooleanExpression> childrenCondList);
	public List<AbstractBooleanExpression> dumpChildrenBooleanExpressions(){
		List<AbstractBooleanExpression> childrenCondList=new ArrayList<AbstractBooleanExpression>(4);
		dumpChildrenBooleanExpressions(childrenCondList);
		return childrenCondList;
	}
	
	public abstract void dumpOwnBooleanExpressions(List<AbstractBooleanExpression> ownCondList);
	public List<AbstractBooleanExpression> dumpOwnBooleanExpressions(){
		List<AbstractBooleanExpression> ownCondList=new ArrayList<AbstractBooleanExpression>(4);
		dumpOwnBooleanExpressions(ownCondList);
		return ownCondList;
	}
	
	public void dumpAllBooleanExpressions(List<AbstractBooleanExpression> allCondList){
		this.dumpOwnBooleanExpressions(allCondList);
		this.dumpChildrenBooleanExpressions(allCondList);
	}
	public List<AbstractBooleanExpression> dumpAllBooleanExpressions(){
		List<AbstractBooleanExpression> allCondList=new ArrayList<AbstractBooleanExpression>(4);
		dumpAllBooleanExpressions(allCondList);
		return allCondList;
	}
	
	public static class StreamAndMapAndBoolComparisonResult implements
		ITuple3D<Stream,Map<EventAlias,EventAlias>,BooleanExpressionComparisonResult>{
		private static final long serialVersionUID = 1331939948512524595L;
		Stream first;
		Map<EventAlias,EventAlias> second;
		transient BooleanExpressionComparisonResult third;
		
		public StreamAndMapAndBoolComparisonResult(){
			super();
		}
		/**
		 * 
		 * @param first
		 * @param second Stream to StreamContainer EventAlias Map
		 * @param third
		 */
		public StreamAndMapAndBoolComparisonResult(Stream first,
				Map<EventAlias, EventAlias> second, 
				BooleanExpressionComparisonResult third) {
			this.first = first;
			this.second = second;
			this.third = third;
		}

		@Override
		public Stream getFirst() {
			return first;
		}

		@Override
		public Map<EventAlias, EventAlias> getSecond() {
			return second;
		}

		@Override
		public BooleanExpressionComparisonResult getThird() {
			return third;
		}

		@Override
		public void setFirst(Stream stream) {
			this.first = stream;
		}

		@Override
		public void setSecond(Map<EventAlias, EventAlias> eaMap) {
			this.second = eaMap;
		}

		@Override
		public void setThird(BooleanExpressionComparisonResult cr) {
			this.third = cr;
		}
		
		@Override
		public String toString(){
			return String.format("[%s,%s,%s]", first.toString(), second.toString(), third.toString());
		}
	}
}
