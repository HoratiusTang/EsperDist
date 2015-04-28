package dist.esper.core.flow.stream;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventOrPropertySpecification;
import dist.esper.epl.expr.TimePeriod;
import dist.esper.epl.expr.ViewSpecification;
import dist.esper.event.Event;

/**
 * the join stream in @StreamTree
 * usually contains two up-streams in childList
 * 
 * @see @JoinNode
 * @author tjy
 *
 */
public class JoinStream extends DerivedStream {
	private static final long serialVersionUID = -375345506016592298L;
	List<Stream> upStreamList=new ArrayList<Stream>(2);
	List<AbstractBooleanExpression> joinExprList=new ArrayList<AbstractBooleanExpression>(2);
		
	public JoinStream() {
		super();
	}
	public JoinStream(List<Stream> upStreamList,
			List<AbstractBooleanExpression> joinExprList) {
		super();
		this.upStreamList = upStreamList;
		this.joinExprList = joinExprList;
	}
	public List<Stream> getUpStreamList() {
		return upStreamList;
	}
	public Stream getUpStream(int index){
		return upStreamList.get(index);
	}
	public int getUpStreamCount(){
		return upStreamList.size();
	}
	public void setUpStreamList(List<Stream> childList) {
		this.upStreamList = childList;
	}
	public void addUpStream(Stream child){
		upStreamList.add(child);
	}
	public List<AbstractBooleanExpression> getJoinExprList() {
		return joinExprList;
	}
	public void setJoinExprList(List<AbstractBooleanExpression> joinExprList) {
		this.joinExprList.addAll(joinExprList);
	}
	
	@Override
	public void toStringBuilder(StringBuilder sw, int indent) {
		StreamStringlizer.toStringBuilder(this, sw, indent);
	}
//	@Override
//	public Stream getChildStreamLocationByEventName(
//			String childEventName) {
//		for(Stream child: upStreamList){
//			if(child.getInternalCompositeEvent().getName().equals(childEventName)){
//				return child;
//			}
//		}
//		return null;
//	}
	@Override
	public int getBaseStreamCount() {
		int count=0;
		for(Stream child: upStreamList){
			count+=child.getBaseStreamCount();
		}
		return count;
	}
	@Override
	public int getRawStreamCount() {
		int count=0;
		for(Stream child: upStreamList){
			count+=child.getRawStreamCount();
		}
		return count;
	}
	
	@Override
	public void dumpEvents(Collection<Event> events) {
		for(Stream child: upStreamList){
			((DerivedStream)child).dumpEvents(events);
		}
	}
	
	@Override
	public void dumpEventAlias(Set<EventAlias> eaSet) {
		for(Stream child: upStreamList){
			((DerivedStream)child).dumpEventAlias(eaSet);
		}
	}
	
	@Override
	public boolean tryAddResultElement(EventOrPropertySpecification eps) {
		boolean childAdded=false;
		for(Stream child: upStreamList){
			if(((DerivedStream)child).tryAddResultElement(eps)){
				childAdded=true;
				break;
			}
		}
		if(childAdded){
			addNewResultElementIfNotExists(eps);
			return true;
		}
		return false;
	}
	@Override
	public void dumpChildrenBooleanExpressions(
			List<AbstractBooleanExpression> childrenCondList) {
		for(Stream child: upStreamList){
			((DerivedStream)child).dumpAllBooleanExpressions(childrenCondList);
		}
	}
	@Override
	public void dumpOwnBooleanExpressions(
			List<AbstractBooleanExpression> ownCondList) {
		ownCondList.addAll(joinExprList);
	}
	@Override
	public ViewSpecification[] getViewSpecs() {
		TimePeriod maxTimePeriod=null;
		for(Stream child: upStreamList){
			ViewSpecification[] vss=((DerivedStream)child).getViewSpecs();
			if(vss!=null){
				TimePeriod t=(TimePeriod)vss[0].getParamList().get(0);
				if(maxTimePeriod==null || TimePeriod.compare(t, maxTimePeriod)>0){
					maxTimePeriod=t;
				}
			}
		}
		return new ViewSpecification[]{new ViewSpecification("win","time", maxTimePeriod)};
	}
	@Override
	public int getLevel() {
		int childMaxLevel=0;
		for(Stream child: upStreamList){
			childMaxLevel=child.getLevel()>childMaxLevel?child.getLevel():childMaxLevel;
		}
		return childMaxLevel+1;
	}
}
