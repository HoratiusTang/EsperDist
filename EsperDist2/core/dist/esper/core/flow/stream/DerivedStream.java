package dist.esper.core.flow.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import dist.esper.core.flow.container.DerivedStreamContainer;
import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.AbstractExpression;
import dist.esper.epl.expr.AbstractResultExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.SelectClauseExpressionElement;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult;
import dist.esper.epl.expr.util.DeepCloneReplaceFactory;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator;
import dist.esper.epl.expr.util.EventOrPropertySpecComparator.EPSRelation;
import dist.esper.event.Event;
import dist.esper.util.Tuple2D;
import dist.esper.util.Tuple3D;

/**
 * the super-class of all internal streams, 
 * distinguished from @RawStream.
 * 
 * @author tjy
 *
 */
public abstract class DerivedStream extends Stream {
	private static final long serialVersionUID = 4391331724004527331L;
	public static EventOrPropertySpecComparator epsComparator=new EventOrPropertySpecComparator();	
	transient List<ContainerAndMapAndBoolComparisonResult> directReusableCMCRList=new ArrayList<ContainerAndMapAndBoolComparisonResult>();
	transient List<ContainerAndMapAndBoolComparisonResult> indirectReusableCMCRList=new ArrayList<ContainerAndMapAndBoolComparisonResult>();
	transient List<ContainerAndMapAndBoolComparisonResult> equalChildrenCMCRList=new ArrayList<ContainerAndMapAndBoolComparisonResult>();
	transient ContainerAndMapAndBoolComparisonResult finalReusingCMCR=null;
	public static final long UNCONCERNED_WINDOW_TIME=-1;
	long windowTimeUS=UNCONCERNED_WINDOW_TIME;
	
	public abstract ViewSpecification[] getViewSpecs();
	
	public DerivedStream() {
		super();		
	}

	public String getWindowTimeViewSpecString(){
		return String.format(".win:time(%d msec)", this.getWindowTimeUS()/1000);
	}
	
	public long getWindowTimeUS() {
		return windowTimeUS;
	}

	public void setWindowTimeUS(long windowTimeUS) {
		this.windowTimeUS = windowTimeUS;
	}

	public ContainerAndMapAndBoolComparisonResult getFinalReusingContainerMapComparisonResult() {
		return finalReusingCMCR;
	}
	
	public void setFinalReusingContainerMapComparisonResult(ContainerAndMapAndBoolComparisonResult finalCMCR) {
		this.finalReusingCMCR = finalCMCR;
	}
	
	public List<ContainerAndMapAndBoolComparisonResult> getEqualChildrenContainerMapComparisonResultList() {
		return equalChildrenCMCRList;
	}	

	public void addEqualChildrenContainerMapComparisonResult(ContainerAndMapAndBoolComparisonResult equalChildrenCMCR) {
		if(!this.equalChildrenCMCRList.contains(equalChildrenCMCR)){
			this.equalChildrenCMCRList.add(equalChildrenCMCR);
		}
	}

	public void addEqualChildrenContainerMapComparisonResult(DerivedStreamContainer pcl, Map<EventAlias,EventAlias> eaMap, BooleanExpressionComparisonResult cr) {
		addEqualChildrenContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(pcl, eaMap, cr));
	}

	public List<ContainerAndMapAndBoolComparisonResult> getDirectReusableContainerMapComparisonResultList() {
		return directReusableCMCRList;
	}	

	public void addDirectReusableContainerMapComparisonResult(ContainerAndMapAndBoolComparisonResult reusableCMCR) {
		if(!this.directReusableCMCRList.contains(reusableCMCR)){
			this.directReusableCMCRList.add(reusableCMCR);
		}
	}

	public void addDirectReusableContainerMapComparisonResult(DerivedStreamContainer pcl, Map<EventAlias,EventAlias> eaMap, BooleanExpressionComparisonResult cr) {
		addDirectReusableContainerMapComparisonResult(new ContainerAndMapAndBoolComparisonResult(pcl, eaMap, cr));
	}

	public List<ContainerAndMapAndBoolComparisonResult> getIndirectReusableContainerMapComparisonResultList() {
		return indirectReusableCMCRList;
	}
	
	public void addIndirectReusableContainerMapComparisonResult(ContainerAndMapAndBoolComparisonResult indirectReusableCMCR){
		if(!this.indirectReusableCMCRList.contains(indirectReusableCMCR)){
			this.indirectReusableCMCRList.add(indirectReusableCMCR);
		}
	}

	public void addIndirectReusableContainerMapComparisonResult(DerivedStreamContainer pcsc, Map<EventAlias,EventAlias> eaMap, BooleanExpressionComparisonResult cr){
		ContainerAndMapAndBoolComparisonResult cm=new ContainerAndMapAndBoolComparisonResult(pcsc, eaMap, cr);
		addIndirectReusableContainerMapComparisonResult(cm);
	}
	
	public ContainerAndMapAndBoolComparisonResult getIndirectReusableContainerMapBoolComparisonResultById(long eplId) {
		for(ContainerAndMapAndBoolComparisonResult cm: indirectReusableCMCRList){
			if(cm.getFirst().getEplId()==eplId){
				return cm;
			}
		}
		return null;
	}
	
	
	public ContainerAndMapAndBoolComparisonResult getDirectOrIndirectReusableContainerMapBoolComparisonResult(DerivedStreamContainer psc) {
		ContainerAndMapAndBoolComparisonResult cm=getDirectReusableContainerMapBoolComparisonResult(psc);
		if(cm==null){
			cm=getIndirectReusableContainerMapBoolComparisonResult(psc);
		}
		return cm;
	}
	
	public ContainerAndMapAndBoolComparisonResult getDirectReusableContainerMapBoolComparisonResult(DerivedStreamContainer psc) {
		for(ContainerAndMapAndBoolComparisonResult cm: directReusableCMCRList){
			if(cm.getFirst()==psc){
				return cm;
			}
		}
		return null;
	}
	
	public ContainerAndMapAndBoolComparisonResult getIndirectReusableContainerMapBoolComparisonResult(DerivedStreamContainer psc) {
		for(ContainerAndMapAndBoolComparisonResult cm: indirectReusableCMCRList){
			if(cm.getFirst()==psc){
				return cm;
			}
		}
		return null;
	}
	
	public boolean hasDirectResuableContainerOnWorker(String workerId){
		for(ContainerAndMapAndBoolComparisonResult cm: directReusableCMCRList){
			if(cm.getFirst().getWorkerId().getId().equals(workerId)){
				return true;
			}
		}
		return false;
	}
	
	public boolean hasIndirectReusableContainerOnWorker(String workerId){
		for(ContainerAndMapAndBoolComparisonResult cm: indirectReusableCMCRList){
			if(cm.getFirst().getWorkerId().getId().equals(workerId)){
				return true;
			}
		}
		return false;
	}
	
	public boolean hasDirectOrIndirectReusableContainerOnWorker(String workerId){
		return hasDirectResuableContainerOnWorker(workerId) || hasIndirectReusableContainerOnWorker(workerId);
	}
	
	public ContainerAndMapAndBoolComparisonResult getDirectReusableContainerMapBoolComparisonResultById(long eplId) {
		for(ContainerAndMapAndBoolComparisonResult cm: directReusableCMCRList){
			if(cm.getFirst().getEplId()==eplId){
				return cm;
			}
		}
		return null;
	}
	
	//public abstract Stream getChildStreamLocationByEventName(String childEventName);

	public abstract boolean tryAddResultElement(EventOrPropertySpecification eps);
	public void addNewResultElementIfNotExists(EventOrPropertySpecification eps){
		boolean found=false;
		for(SelectClauseExpressionElement se: this.getResultElementList()){
//			int r=EventOrPropertySpecification.getRelation((EventOrPropertySpecification)se.getSelectExpr(), eps);
//			if(r==EventOrPropertySpecification.Relation.CONTAINS ||
//				r==EventOrPropertySpecification.Relation.EQUAL){
//				found=true;
//				break;
//			}
			EPSRelation r=epsComparator.compare((EventOrPropertySpecification)se.getSelectExpr(), eps);
			if(r==EPSRelation.CONTAINS || r==EPSRelation.EQUAL){
				found=true;
				break;
			}
		}
		if(!found){
			this.addResultElement(new SelectClauseExpressionElement(eps));
		}
	}
	
	public abstract void dumpEvents(Collection<Event> events);
	public Collection<Event> dumpEvents(){
		List<Event> events=new ArrayList<Event>(4);
		this.dumpEvents(events);
		return events;
	}
	
	public abstract void dumpEventAlias(Set<EventAlias> eaSet);	
	public Set<EventAlias> dumpEventAlias(){
		Set<EventAlias> eaSet=new HashSet<EventAlias>();
		this.dumpEventAlias(eaSet);
		return eaSet;
	}
	
	public abstract void dumpChildrenBooleanExpressions(List<AbstractBooleanExpression> childrenCondList);
	public List<AbstractBooleanExpression> dumpChildrenBooleanExpressions(){
		List<AbstractBooleanExpression> childrenCondList=new ArrayList<AbstractBooleanExpression>(4);
		dumpChildrenBooleanExpressions(childrenCondList);
		return childrenCondList;
	}
	
	public abstract void dumpOwnBooleanExpressions(List<AbstractBooleanExpression> ownCondList);
	public List<AbstractBooleanExpression> dumpCurrentBooleanExpressions(){
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
	
	public static class ContainerAndMapAndBoolComparisonResult 
		extends Tuple3D<DerivedStreamContainer, Map<EventAlias,EventAlias>, BooleanExpressionComparisonResult>{		
		private static final long serialVersionUID = 2433427890850493541L;
		public ContainerAndMapAndBoolComparisonResult(){
			super();
		}
		public ContainerAndMapAndBoolComparisonResult(DerivedStreamContainer first,
				Map<EventAlias, EventAlias> second, BooleanExpressionComparisonResult third) {
			super(first, second, third);
		}
	}
	public static class ContainerAndMapAndBoolComparisonResultOfAgent extends ContainerAndMapAndBoolComparisonResult{		
		private static final long serialVersionUID = -2958522508383488944L;
		ContainerAndMapAndBoolComparisonResult agentCMCR;
		
		public ContainerAndMapAndBoolComparisonResultOfAgent(){
			super();
		}
		public ContainerAndMapAndBoolComparisonResultOfAgent(
				ContainerAndMapAndBoolComparisonResult cmcr,
				ContainerAndMapAndBoolComparisonResult agentCMCR) {
			super(cmcr.getFirst(), cmcr.getSecond(), cmcr.getThird());
			this.agentCMCR=agentCMCR;
		}
		public ContainerAndMapAndBoolComparisonResult getAgentContainerAndMapAndBoolComparisonResult() {
			return agentCMCR;
		}
		public void setAgentContainerAndMapAndBoolComparisonResult(ContainerAndMapAndBoolComparisonResult agentCMCR) {
			this.agentCMCR = agentCMCR;
		}
		public ContainerAndMapAndBoolComparisonResult getAgentCMCR() {
			return agentCMCR;
		}
		public void setAgentCMCR(ContainerAndMapAndBoolComparisonResult agentCMCR) {
			this.agentCMCR = agentCMCR;
		}
	}
	public List<ContainerAndMapAndBoolComparisonResult> getDirectReusableCMCRList() {
		return directReusableCMCRList;
	}

	public void setDirectReusableCMCRList(
			List<ContainerAndMapAndBoolComparisonResult> directReusableCMCRList) {
		this.directReusableCMCRList = directReusableCMCRList;
	}

	public List<ContainerAndMapAndBoolComparisonResult> getIndirectReusableCMCRList() {
		return indirectReusableCMCRList;
	}

	public void setIndirectReusableCMCRList(
			List<ContainerAndMapAndBoolComparisonResult> indirectReusableCMCRList) {
		this.indirectReusableCMCRList = indirectReusableCMCRList;
	}

	public ContainerAndMapAndBoolComparisonResult getFinalReusingCMCR() {
		return finalReusingCMCR;
	}

	public void setFinalReusingCMCR(ContainerAndMapAndBoolComparisonResult finalCMCR) {
		this.finalReusingCMCR = finalCMCR;
	}

	public List<ContainerAndMapAndBoolComparisonResult> getEqualChildrenCMCRList() {
		return equalChildrenCMCRList;
	}

	public void setEqualChildrenCMCRList(
			List<ContainerAndMapAndBoolComparisonResult> equalChildrenCMCRList) {
		this.equalChildrenCMCRList = equalChildrenCMCRList;
	}
	
}
