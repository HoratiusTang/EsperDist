package dist.esper.epl.expr.util;

import java.util.*;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.pattern.*;

public class ExpressionComparator {
	public enum CompareStrategy{
		EVENT_MATCH,
		EVENTALIAS_MATCH,
		REPLACE_EVENTALIAS_MATCH,
	}
	//boolean ignoreEventAliasName=false;
	CompareStrategy compareStrategy=CompareStrategy.EVENT_MATCH;
	Map<EventAlias, EventAlias> eaMap=new HashMap<EventAlias, EventAlias>(2);//replace the second EventAlias
	Map<String, Comparator<?>> compMap=new HashMap<String, Comparator<?>>();  
	
	public ExpressionComparator(){
		this(CompareStrategy.REPLACE_EVENTALIAS_MATCH, null);
	}
	
	public ExpressionComparator(CompareStrategy compareStrategy, Map<EventAlias, EventAlias> eventAliasMap) {
		super();
		this.compareStrategy = compareStrategy;
		if(eventAliasMap!=null){
			this.eaMap=eventAliasMap;
		}
		
		compMap.put(ComparisonExpression.class.getSimpleName(), new ComparisonExpressionComparator());
		compMap.put(CompositeExpression.class.getSimpleName(), new CompositeExpressionComparator());
		compMap.put(TimePeriod.class.getSimpleName(), new TimePeriodComparator());
		compMap.put(Value.class.getSimpleName(), new ValueComparator());
		compMap.put(AggregationExpression.class.getSimpleName(), new AggregationExpressionComparator());
		compMap.put(MathExpression.class.getSimpleName(), new MathExpressionComparator());
		compMap.put(UDFDotExpressionItem.class.getSimpleName(), new UDFChainedSegmentComparator());
		compMap.put(UDFDotExpression.class.getSimpleName(), new UDFDotExpressionComparator());
		
		compMap.put(EventSpecification.class.getSimpleName(), new EventSpecificationComparator());
		compMap.put(EventIndexedSpecification.class.getSimpleName(), new EventIndexedSpecificationComparator());
		compMap.put(EventPropertySpecification.class.getSimpleName(), new EventPropertySpecificationComparator());
		compMap.put(EventPropertyIndexedSpecification.class.getSimpleName(), new EventPropertyIndexedSpecificationComparator());
		
		/** FIXME: each PatternNode should have individual comparator
		Comparator<?> pmComp=new PatternMultiChildNodeComparator();
		compMap.put(PatternAndNode.class.getSimpleName(), pmComp);
		compMap.put(PatternFollowedByNode.class.getSimpleName(), pmComp);
		compMap.put(PatternOrNode.class.getSimpleName(), pmComp);
		
		Comparator<?> psComp=new PatternSingleChildNodeComparator();
		compMap.put(PatternEveryNode.class.getSimpleName(), psComp);
		compMap.put(PatternGuardNode.class.getSimpleName(), psComp);
		compMap.put(PatternMatchUntilNode.class.getSimpleName(), psComp);
		compMap.put(PatternNotNode.class.getSimpleName(), psComp);
		compMap.put(PatternObserverNode.class.getSimpleName(), psComp);
		
		compMap.put(PatternFilterNode.class.getSimpleName(), new PatternFilterNodeComparator());
		**/
	}
	
	public CompareStrategy getCompareStrategy() {
		return compareStrategy;
	}

	public void setCompareStrategy(CompareStrategy compareStrategy) {
		this.compareStrategy = compareStrategy;
	}

	public Map<EventAlias, EventAlias> getEventAliasMap() {
		return eaMap;
	}

	public void setEventAliasMap(Map<EventAlias, EventAlias> eaMap) {
		this.eaMap = eaMap;
	}

	public boolean compare(AbstractExpression a, AbstractExpression b){
		if(a==null && b==null){
			return true;
		}
		else if(a!=null && b!=null){
			Comparator<?> comp=compMap.get(a.getClass().getSimpleName());
			if(comp==null){
				throw new RuntimeException("not implemented yet");
			}
			return comp.compare1(a, b);
		}
		return false;
	}
	
	public boolean compare(EventAlias a, EventAlias b){
		if(a==null && b==null){
			return true;
		}
		else if(a!=null && b!=null){
			if(a.getEvent().equals(b.getEvent())){
				if(compareStrategy==CompareStrategy.EVENT_MATCH){
					return true;
				}
				
				EventAlias b2=b;
				if(compareStrategy==CompareStrategy.REPLACE_EVENTALIAS_MATCH){
					b2=eaMap.get(b);
					if(b2==null){
						return false;
					}
				}
				if(a.getEventAsName()==null && b2.getEventAsName()==null){
					return true;
				}
				else if((a.getEventAsName()!=null && b2.getEventAsName()!=null) &&
						a.getEventAsName().equals(b2.getEventAsName())){
					return true;
				}
			}
		}
		return false;
	}
	
	abstract class Comparator<T extends AbstractExpression>{
		@SuppressWarnings("unchecked")
		public boolean compare1(AbstractExpression a, AbstractExpression b){
			try{
				return compare2((T)a, (T)b);
			}
			catch(Exception ex){
				//System.err.format("compare between different type: %s, %s\n", a.getClass().getSimpleName(), b.getClass().getSimpleName());
				return false;
			}
		}
		public abstract boolean compare2(T a, T b);
	}
	
	class ComparisonExpressionComparator extends Comparator<ComparisonExpression>{
		@Override
		public boolean compare2(ComparisonExpression a, ComparisonExpression b) {
			boolean result=false;
			if(a.getRelation()==b.getRelation()){
				if(compare(a.getChildExprList().get(0), b.getChildExprList().get(0)) && 
					compare(a.getChildExprList().get(1), b.getChildExprList().get(1))){
					result=true;
				}
			}
			if(!result && a.getRelation()==b.getRelation().reverse()){
				if(compare(a.getChildExprList().get(0), b.getChildExprList().get(1)) && 
					compare(a.getChildExprList().get(1), b.getChildExprList().get(0))){
					result=true;
				}
			}
			return result;
		}
	}
	
	class CompositeExpressionComparator extends Comparator<CompositeExpression>{
		@Override
		public boolean compare2(CompositeExpression a, CompositeExpression b) {
			if(a.getRelation()==b.getRelation() &&
				a.getChildExprList().size()==b.getChildExprList().size()){
				boolean[] aFlag=new boolean[a.getChildExprList().size()];
				boolean[] bFlag=new boolean[b.getChildExprList().size()];
				Arrays.fill(aFlag,false);
				Arrays.fill(bFlag,false);
				for(int i=0; i<a.getChildExprList().size(); i++){
					if(aFlag[i]){continue;}
					for(int j=0; j<b.getChildExprList().size(); j++){
						if(bFlag[j]){continue;}
						if(compare(a.getChildExprList().get(i), b.getChildExprList().get(j))){
							aFlag[i]=true;
							bFlag[j]=true;
						}
					}
				}
				for(int i=0; i<a.getChildExprList().size(); i++){
					if(!aFlag[i]){
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}
	
	class TimePeriodComparator extends Comparator<TimePeriod>{
		@Override
		public boolean compare2(TimePeriod a, TimePeriod b) {
			return a.equals(b);
		}
	}
	
	class ValueComparator extends Comparator<Value>{
		@Override
		public boolean compare2(Value a, Value b) {
			return Value.compareValue(a, b)==0;
		}
	}
	
	class AggregationExpressionComparator extends Comparator<AggregationExpression>{
		@Override
		public boolean compare2(AggregationExpression a, AggregationExpression b) {
			if(a.getAggType()==b.getAggType() &&
					compare(a.getExpr(), b.getExpr()) &&
					a.isDistinct()==b.isDistinct()){
				if(a.getFilterExpr()==null && b.getFilterExpr()==null){
					return true;
				}
				else if(a.getFilterExpr()!=null && 
						b.getFilterExpr()!=null && 
						compare(a.getFilterExpr(), b.getFilterExpr())){
						return true;
				}
			}
			return false;
		}
	}
	
	class MathExpressionComparator extends Comparator<MathExpression>{
		@Override
		public boolean compare2(MathExpression a, MathExpression b) {
			if(a.getOperationType()==b.getOperationType() &&
				a.getChildExprList().size()==b.getChildExprList().size()){
				for(int i=0; i<a.getChildExprList().size(); i++){
					if(!compare(a.getChildExprList().get(i), b.getChildExprList().get(i))){
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}
	
	class UDFDotExpressionComparator extends Comparator<UDFDotExpression>{
		@Override
		public boolean compare2(UDFDotExpression a, UDFDotExpression b) {
			if(a.getItemList().size()==b.getItemList().size()){
				for(int i=0; i<a.getItemList().size(); i++){
					if(!compare(a.getItemList().get(i), b.getItemList().get(i))){
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}
	
	class UDFChainedSegmentComparator extends Comparator<UDFDotExpressionItem>{

		@Override
		public boolean compare2(UDFDotExpressionItem a, UDFDotExpressionItem b) {
			if(a.getName().equals(b.getName()) && a.isProperty()==b.isProperty()){
				for(int i=0; i<a.getParamList().size(); i++){
					if(!compare(a.getParamList().get(i), b.getParamList().get(i))){
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}
	
	class EventSpecificationComparator extends Comparator<EventSpecification>{
		@Override
		public boolean compare2(EventSpecification a, EventSpecification b) {
			if(a.getClass().getSimpleName().equals(b.getClass().getSimpleName())){
				if(compare(a.getEventAlias(), b.getEventAlias()) && 
					compare(a.getOwnEventAlias(), b.getOwnEventAlias())){
					return true;
				}
			}
			return false;
		}
	}
	
	class EventIndexedSpecificationComparator extends Comparator<EventIndexedSpecification>{
		@Override
		public boolean compare2(EventIndexedSpecification a, EventIndexedSpecification b) {
			if(a.getClass().getSimpleName().equals(b.getClass().getSimpleName())){
				if(compare(a.getEventAlias(), b.getEventAlias()) && 
					compare(a.getOwnEventAlias(), b.getOwnEventAlias()) &&
					a.getIndex()==b.getIndex()){
					return true;
				}
			}
			return false;
		}
	}
	
	class EventPropertySpecificationComparator extends Comparator<EventPropertySpecification>{
		@Override
		public boolean compare2(EventPropertySpecification a, EventPropertySpecification b) {
			if(a.getClass().getSimpleName().equals(b.getClass().getSimpleName())){
				if(a.getEventProp().equals(b.getEventProp()) && 
					compare(a.getEventSpec(), b.getEventSpec())){
					return true;
				}
			}
			return false;
		}
	}
	
	class EventPropertyIndexedSpecificationComparator extends Comparator<EventPropertyIndexedSpecification>{
		@Override
		public boolean compare2(EventPropertyIndexedSpecification a, EventPropertyIndexedSpecification b) {
			if(a.getClass().getSimpleName().equals(b.getClass().getSimpleName())){
				if( a.getIndex()==b.getIndex() &&
					a.getEventProp().equals(b.getEventProp()) &&
					compare(a.getEventSpec(), b.getEventSpec())){
					return true;
				}
			}
			return false;
		}
	}
	
	class PatternMultiChildNodeComparator extends Comparator<PatternMultiChildNode>{
		@Override
		public boolean compare2(PatternMultiChildNode a, PatternMultiChildNode b) {
			if(a.getClass().getSimpleName().equals(b.getClass().getSimpleName()) &&
				a.getChildNodeList().size()==b.getChildNodeList().size()){
				for(int i=0; i<a.getChildNodeList().size(); i++){
					if(!compare(a.getChildNodeList().get(i), b.getChildNodeList().get(i))){
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}
	
	class PatternFilterNodeComparator extends Comparator<PatternFilterNode>{
		@Override
		public boolean compare2(PatternFilterNode a, PatternFilterNode b) {
			throw new RuntimeException("not implemented yet");
		}
	}
	
	class PatternSingleChildNodeComparator extends Comparator<PatternSingleChildNode>{
		@Override
		public boolean compare2(PatternSingleChildNode a, PatternSingleChildNode b) {
			if(a.getClass().getSimpleName().equals(b.getClass().getSimpleName())){
				if(compare(a.getChildNode(), b.getChildNode())){
					return true;
				}
			}
			return false;
		}
	}
}
