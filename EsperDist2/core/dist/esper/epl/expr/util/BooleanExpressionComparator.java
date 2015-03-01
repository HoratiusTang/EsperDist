package dist.esper.epl.expr.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import dist.esper.epl.expr.*;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.BooleanExpressionComparisonResultNone;
import dist.esper.epl.expr.util.BooleanExpressionComparisonResult.State;

public class BooleanExpressionComparator {
	//Map<EventAlias, EventAlias> eaMap;//replace second to first
	ExpressionComparator exprComp;//FIXME

	public BooleanExpressionComparator(ExpressionComparator exprComp){
		super();
		this.exprComp=exprComp;
	}
	public BooleanExpressionComparator(Map<EventAlias, EventAlias> eaMap) {
		super();
		this.exprComp=new ExpressionComparator(ExpressionComparator.CompareStrategy.REPLACE_EVENTALIAS_MATCH, eaMap);//FIXME
	}
	
	public ExpressionComparator getExprComparator() {
		return exprComp;
	}
	public void setExprComparator(ExpressionComparator exprComp) {
		this.exprComp = exprComp;
	}

	public BooleanExpressionComparisonResult compare(AbstractBooleanExpression a, AbstractBooleanExpression b){
		return compare(a,b,0);
	}
	public BooleanExpressionComparisonResult compare(AbstractBooleanExpression a, AbstractBooleanExpression b, int deepth){
		if(deepth<=0){
			deepth=0;
			List<AbstractBooleanExpression> bExprList1=a.dumpConjunctionExpressions();
			List<AbstractBooleanExpression> bExprList2=b.dumpConjunctionExpressions();
			return compareConjunctionLists(bExprList1, bExprList2, deepth);
		}
		else{//deepth>=1, CompositeExpression should be OR
			if((a instanceof CompositeExpression) && (b instanceof CompositeExpression)){
				return compareComposisteExpressions((CompositeExpression)a, (CompositeExpression)b, deepth);
			}
			else if((a instanceof ComparisonExpression) && (b instanceof ComparisonExpression)){
				return compareComparisonExpressions((ComparisonExpression)a, (ComparisonExpression)b);
			}
			else if((a instanceof CompositeExpression) && (b instanceof ComparisonExpression)){
				CompositeExpression b2=new CompositeExpression(RelationTypeEnum.OR);
				b2.addChildExpr(b);
				return compareComposisteExpressions((CompositeExpression)a, b2, deepth);
			}
			else if((a instanceof ComparisonExpression) && (b instanceof CompositeExpression)){
				CompositeExpression a2=new CompositeExpression(RelationTypeEnum.OR);
				a2.addChildExpr(a);
				return compareComposisteExpressions(a2, (CompositeExpression)b, deepth);
			}
		}
		return BooleanExpressionComparisonResultNone.getInstance();
	}
	
	/**
	 * assume in each list, the element is not equal or compatible
	 */
	public BooleanExpressionComparisonResult compareConjunctionLists(List<AbstractBooleanExpression> bExprList1, List<AbstractBooleanExpression> bExprList2, int deepth){
		if(bExprList1.size()<bExprList2.size()){
			return BooleanExpressionComparisonResultNone.getInstance();
		}
		
		BooleanExpressionComparisonResult[][] crs=new BooleanExpressionComparisonResult[bExprList1.size()][];
		for(int i=0; i<crs.length; i++){
			crs[i]=new BooleanExpressionComparisonResult[bExprList2.size()];
		}
		
		int[] flag1=new int[bExprList1.size()];
		Arrays.fill(flag1,-1);
		for(int i=0; i<bExprList1.size(); i++){
			for(int j=0; j<bExprList2.size(); j++){
				crs[i][j]=compare(bExprList1.get(i), bExprList2.get(j), deepth+1);
				if(crs[i][j].getOwnState()==State.EQUIVALENT || crs[i][j].getOwnState()==State.IMPLYING){
					flag1[i]=j;
				}
			}
		}
				
		for(int j=0; j<bExprList2.size(); j++){
			State s=State.NONE;
			for(int i=0; i<bExprList1.size(); i++){				
				if(crs[i][j].getOwnState().getId()>s.getId()){
					s=crs[i][j].getOwnState();
					break;
				}
			}
			if(s==State.NONE){			
				return BooleanExpressionComparisonResultNone.getInstance();
			}
		}		
		BooleanExpressionComparisonResult cr=new BooleanExpressionComparisonResult();
		for(int i=0; i<bExprList1.size(); i++){
			if(flag1[i]>=0){
				cr.addOwnPairList(crs[i][flag1[i]].getOwnPairList());
			}
			else{
				cr.addOwnPair(bExprList1.get(i), null, State.SURPLUS);
			}
		}
		return cr;
//		int[] flag1=new int[bExprList1.size()];
//		int[] flag2=new int[bExprList2.size()];
//		Arrays.fill(flag1,-1);
//		Arrays.fill(flag2,-1);
				
//		for(int j=0; j<bExprList2.size(); j++){
//			for(int i=0; i<bExprList1.size(); i++){
//				if(flag1[i]>=0) continue;
//				if(crs[i][j]!=null && 
//					(crs[i][j].getOwnState()==State.EQUAL || crs[i][j].getOwnState()==State.COMPATIBLE)){
//					flag1[i]=j;
//					flag2[j]=i;
//					break;
//				}
//			}
//		}
//		State totalState=State.EQUAL;
//		for(int j=0; j<bExprList2.size(); j++){
//			if(flag2[j]>=0){
//				if(crs[flag2[j]][j].getOwnState()==State.COMPATIBLE){
//					totalState=State.COMPATIBLE;
//				}
//			}
//			else{
//				totalState=State.NONE;
//				break;
//			}
//		}
//		if(totalState==State.EQUAL || totalState==State.COMPATIBLE){
//			BooleanExpressionComparisonResult cr=new BooleanExpressionComparisonResult();
//			for(int i=0; i<bExprList1.size(); i++){
//				if(flag1[i]>=0){
//					cr.addOwnPairList(crs[i][flag1[i]].getOwnPairList());
//				}
//				else{
//					cr.addOwnPair(bExprList1.get(i), null, State.SURPLUS);
//				}
//			}
//			return cr;
//		}
//		return BooleanExpressionComparisonResultNone.getInstance();
	}
	
	public BooleanExpressionComparisonResult compareComposisteExpressions(CompositeExpression a, CompositeExpression b, int deepth){
		if(deepth<=0){
			return compare(a,b,0);
		}
		//a and b both OR, and sub expressions are ComparisionExpression
		assert(a.getRelation()==RelationTypeEnum.OR && b.getRelation()==RelationTypeEnum.OR);
		if(a.getChildExprCount()>b.getChildExprCount()){
			return BooleanExpressionComparisonResultNone.getInstance();
		}
		
		BooleanExpressionComparisonResult[][] crs=new BooleanExpressionComparisonResult[a.getChildExprCount()][];
		for(int i=0; i<crs.length; i++){
			crs[i]=new BooleanExpressionComparisonResult[b.getChildExprCount()];
		}
		
		int equalCount=0;
		//int compatibleCount=0;
		for(int i=0; i<a.getChildExprCount(); i++){
			State s=State.NONE;
			for(int j=0; j<b.getChildExprCount(); j++){//FIXME: not necessary to compute all
				crs[i][j]=compareComparisonExpressions((ComparisonExpression)a.getChildExpr(i), 
														(ComparisonExpression)b.getChildExpr(j));//can't be State.SURPLUS
				if(crs[i][j].getOwnState().getId() > s.getId()){
					s=crs[i][j].getOwnState();
				}
			}
			if(s==State.EQUIVALENT){
				equalCount++;
			}			
			else if(s==State.NONE){//NONE
				return BooleanExpressionComparisonResultNone.getInstance();
			}
		}
		State totalState=State.IMPLYING;
		if(equalCount==b.getChildExprCount()){// && a.getChildExprCount()==b.getChildExprCount()){
			totalState=State.EQUIVALENT;
		}
		
		AbstractBooleanExpression a0=a;
		AbstractBooleanExpression b0=b;
		if(a.getChildExprCount()==1){
			a0=a.getChildExpr(0);
		}
		if(b.getChildExprCount()==1){
			b0=b.getChildExpr(0);
		}
		BooleanExpressionComparisonResult cr=new BooleanExpressionComparisonResult();
		cr.addOwnPair(a0, b0, totalState);
		return cr;
//		int[] flag1=new int[a.getChildExprCount()];
//		int[] flag2=new int[b.getChildExprCount()];
//		Arrays.fill(flag1,-1);
//		Arrays.fill(flag2,-1);
//		
//		for(int i=0; i<a.getChildExprCount(); i++){
//			for(int j=0; j<b.getChildExprCount(); j++){
//				if(flag2[j]>=0) continue;
//				if(crs[i][j]!=null && 
//					(crs[i][j].getOwnState()==State.EQUAL || crs[i][j].getOwnState()==State.COMPATIBLE)){
//					flag1[i]=j;
//					flag2[j]=i;
//					break;
//				}
//			}
//		}
//		
//		State totalState=State.EQUAL;
//		for(int i=0; i<a.getChildExprCount(); i++){
//			if(flag1[i]>=0){
//				if(crs[i][flag1[i]].getOwnState()==State.COMPATIBLE){
//					totalState=State.COMPATIBLE;
//				}
//			}
//			else{
//				totalState=State.NONE;
//				break;
//			}
//		}
//		if(totalState==State.EQUAL || totalState==State.COMPATIBLE){
//			BooleanExpressionComparisonResult cr=new BooleanExpressionComparisonResult();
//			cr.addOwnPair(a, b, totalState);
//			return cr;
//		}
//		return BooleanExpressionComparisonResultNone.getInstance();
	}
	
	/**	 
	 * @param a
	 * @param b
	 * @return if a='id>10' and b='id>5', then turn true 
	 */
	public BooleanExpressionComparisonResult compareComparisonExpressions(ComparisonExpression a, ComparisonExpression b){//a>b, already ordered
		BooleanExpressionComparisonResult cr=BooleanExpressionComparisonResultNone.getInstance();
		if(a.getRelation()==OperatorTypeEnum.EQUAL && b.getRelation()==OperatorTypeEnum.EQUAL){
			if(exprComp.compare(a, b)){
				cr=new BooleanExpressionComparisonResult();
				cr.addOwnPair(a, b, State.EQUIVALENT);
				return cr;
			}
		}
		if(!(a.getChild(0) instanceof EventOrPropertySpecification)){
			a.reverse();
		}
		if(a.getChild(0) instanceof EventOrPropertySpecification){
			if(exprComp.compare(a.getChild(0), b.getChild(1))){
				b.reverse();
			}
			if(exprComp.compare(a.getChild(0), b.getChild(0))){
				int r=-1;
				if(exprComp.compare(a.getChild(1), b.getChild(1))){
					r=checkImplication(a.getRelation(), b.getRelation(), 0);
				}
				else if((a.getChild(1) instanceof Value) && (b.getChild(1) instanceof Value)){//v1!=v2
					r=checkImplication(a.getRelation(), b.getRelation(), Value.compareValue((Value)a.getChild(1), (Value)b.getChild(1)));
				}
				if(r>0){
					cr=new BooleanExpressionComparisonResult();
					cr.addOwnPair(a, b, State.IMPLYING);
				}
				else if(r==0){
					cr=new BooleanExpressionComparisonResult();
					cr.addOwnPair(a, b, State.EQUIVALENT);
				}
				return cr;
			}
		}
		return cr;
	}
	
	private int checkImplication(OperatorTypeEnum op1, OperatorTypeEnum op2, int v1MinusV2){
		if(op1==op2 && v1MinusV2==0){
			return 0;
		}
		
		if(op2==OperatorTypeEnum.GREATER){
			if((op1==OperatorTypeEnum.GREATER && v1MinusV2>0) || 
				((op1==OperatorTypeEnum.GREATER_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && v1MinusV2>0)){
				return 1;
			}
		}
		else if(op2==OperatorTypeEnum.GREATER_OR_EQUAL){
			if((op1==OperatorTypeEnum.GREATER || op1==OperatorTypeEnum.GREATER_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && v1MinusV2>=0){
				return 1;
			}
		}
		else if(op2==OperatorTypeEnum.LESS){
			if((op1==OperatorTypeEnum.LESS && v1MinusV2<0) || 
				((op1==OperatorTypeEnum.LESS_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && v1MinusV2<0)){
				return 1;
			}
		}
		else if(op2==OperatorTypeEnum.LESS_OR_EQUAL){
			if((op1==OperatorTypeEnum.LESS || op1==OperatorTypeEnum.LESS_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && v1MinusV2<=0){
				return 1;
			}
		}
		return -1;
	}
	
//	private boolean checkImplication(OperatorTypeEnum op1, Value v1, OperatorTypeEnum op2, Value v2){
//		int cmpVal=Value.compareValue(v1, v2);
//		if(op2==OperatorTypeEnum.GREATER){
//			if((op1==OperatorTypeEnum.GREATER && cmpVal>=0) || 
//				((op1==OperatorTypeEnum.GREATER_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && cmpVal>0)){
//				return true;
//			}
//		}
//		else if(op2==OperatorTypeEnum.GREATER_OR_EQUAL){
//			if((op1==OperatorTypeEnum.GREATER || op1==OperatorTypeEnum.GREATER_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && cmpVal>=0){
//				return true;
//			}
//		}
//		else if(op2==OperatorTypeEnum.LESS){
//			if((op1==OperatorTypeEnum.LESS && cmpVal<=0) || 
//				((op1==OperatorTypeEnum.LESS_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && cmpVal<0)){
//				return true;
//			}
//		}
//		else if(op2==OperatorTypeEnum.LESS_OR_EQUAL){
//			if((op1==OperatorTypeEnum.LESS || op1==OperatorTypeEnum.LESS_OR_EQUAL || op1==OperatorTypeEnum.EQUAL) && cmpVal<=0){
//				return true;
//			}
//		}
//		return false;
//	}
//	
//	private boolean checkImplicationOperator(OperatorTypeEnum op1, OperatorTypeEnum op2){
//		if(op2==OperatorTypeEnum.GREATER && op1==OperatorTypeEnum.GREATER){
//			return true;
//		}
//		else if(op2==OperatorTypeEnum.GREATER_OR_EQUAL && (op1==OperatorTypeEnum.GREATER_OR_EQUAL || op1==OperatorTypeEnum.GREATER)){
//			return true;
//		}
//		else if(op2==OperatorTypeEnum.LESS && op1==OperatorTypeEnum.LESS){
//			return true;
//		}
//		else if(op2==OperatorTypeEnum.LESS_OR_EQUAL && (op1==OperatorTypeEnum.LESS_OR_EQUAL || op1==OperatorTypeEnum.LESS)){
//			return true;
//		}
//		else if(op2==OperatorTypeEnum.EQUAL && op1==OperatorTypeEnum.EQUAL){
//			return true;
//		}
//		return false;
//	}
}
