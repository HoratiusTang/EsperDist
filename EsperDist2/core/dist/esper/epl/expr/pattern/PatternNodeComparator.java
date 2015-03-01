package dist.esper.epl.expr.pattern;

import java.util.ArrayList;
import java.util.List;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.epl.expr.ComparisonExpression;
import dist.esper.epl.expr.EventAlias;
import dist.esper.epl.expr.EventSpecification;

public class PatternNodeComparator {
	static class EqualObjectPair{
		Object first;
		Object second;
	}
	
	List<EqualObjectPair> equalPairList=new ArrayList<EqualObjectPair>(4);
	
	public boolean compare(EventAlias ea1, EventAlias ea2){
		return false;
	}
	
	public boolean compare(EventSpecification es1, EventSpecification es2){
		return false;
	}
	
	public boolean compare(AbstractBooleanExpression be1, AbstractBooleanExpression be2){
		return false;
	}
	
	public boolean compare(ComparisonExpression ce1, ComparisonExpression ce2){
		if(ce1.getRelation()==ce2.getRelation()){
			
		}
		return false;
	}
	
	public boolean compare(PatternFilterNode fn1, PatternFilterNode fn2){
		if(compare(fn1.getEventAlias(), fn2.getEventAlias())){
			//if(compare(fn1.get))
		}
		return false;
	}
}
