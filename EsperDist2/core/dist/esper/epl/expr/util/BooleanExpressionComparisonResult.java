package dist.esper.epl.expr.util;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import dist.esper.epl.expr.AbstractBooleanExpression;
import dist.esper.util.Tuple2D;

public class BooleanExpressionComparisonResult implements Serializable{
	private static final long serialVersionUID = 1L;

	public enum State{
		NONE("none",0),
		EQUIVALENT("equivalent",1),
		IMPLYING("implying",2),
		SURPLUS("surplus",3);
		
		String string;
		int id;
		State(String str, int id){
			this.string=str;
			this.id=id;
		}
		@Override
		public String toString() {
			return string;
		}
		public int getId() {
			return id;
		}
		public String getString() {
			return string;
		}
		public void setString(String string) {
			this.string = string;
		}
		public void setId(int id) {
			this.id = id;
		}
	}
	
	public static class BooleanExpressionComparisonPair extends Tuple2D<AbstractBooleanExpression,AbstractBooleanExpression>{		
		private static final long serialVersionUID = -6145032746759552160L;
		State state;
		
		public BooleanExpressionComparisonPair() {
			super();
		}
		public BooleanExpressionComparisonPair(AbstractBooleanExpression first,
				AbstractBooleanExpression second, State state) {
			super(first, second);
			this.state=state;
		}
		public State getState() {
			return state;
		}
		public void setState(State state) {
			this.state = state;
		}
		@Override
		public String toString(){
			StringBuilder sb=new StringBuilder();
			first.toStringBuilder(sb);
			sb.append(":");
			if(second!=null){
				second.toStringBuilder(sb);
			}
			else{
				sb.append("null");
			}
			sb.append("=");
			sb.append(state.toString());
			return sb.toString();
		}
	}
	
	//State state=State.NONE;
	List<BooleanExpressionComparisonPair> ownPairList;
	List<BooleanExpressionComparisonPair> childPairList;
	
	public BooleanExpressionComparisonResult() {
		super();
		ownPairList=new ArrayList<BooleanExpressionComparisonPair>(4);
		childPairList=new ArrayList<BooleanExpressionComparisonPair>(4);
	}
	public State getOwnState() {
		return getPairListState(ownPairList);
	}
	
	public State getChildrenState(){
		return getPairListState(childPairList);
	}
	
	public int getImplyingAndSurplusComparisonExpressionCount(){
		int count=0;
		for(BooleanExpressionComparisonPair pair: ownPairList){
			if(pair.getState()==State.IMPLYING || 
				pair.getState()==State.SURPLUS){
				count+=pair.getFirst().getComparisonExpressionCount();
			}
		}
		for(BooleanExpressionComparisonPair pair: childPairList){
			if(pair.getState()==State.IMPLYING || 
				pair.getState()==State.SURPLUS){
				count+=pair.getFirst().getComparisonExpressionCount();
			}
		}
		return count;
	}
	
	public State getTotalState(){
		State ownState=getOwnState();
		State childrenState=getChildrenState();
		if(ownState==State.NONE || childrenState==State.NONE){
			return State.NONE;
		}
		if(ownState.getId()>childrenState.getId()){
			return ownState;
		}
		else{
			return childrenState;
		}
	}
	
	private static State getPairListState(List<BooleanExpressionComparisonPair> pairList){
		State state=State.EQUIVALENT;
		for(BooleanExpressionComparisonPair pair: pairList){
			if(pair.getState().getId()>state.getId()){
				state=pair.getState();
			}
			else if(pair.getState()==State.NONE){
				return State.NONE;
			}
		}
		if(state==State.SURPLUS){
			state=State.IMPLYING;
		}
		return state;
	}
	
	public void setOwnPairList(List<BooleanExpressionComparisonPair> ownPairList) {
		this.ownPairList = ownPairList;
	}
	public void setChildPairList(List<BooleanExpressionComparisonPair> childPairList) {
		this.childPairList = childPairList;
	}
	public List<BooleanExpressionComparisonPair> getOwnPairList() {
		return ownPairList;
	}
	public List<BooleanExpressionComparisonPair> getChildPairList() {
		return childPairList;
	}
	public void addOwnPair(AbstractBooleanExpression a, AbstractBooleanExpression b, State state){
		BooleanExpressionComparisonPair pair=new BooleanExpressionComparisonPair(a,b,state);
		addOwnPair(pair);
	}
	public void addOwnPair(BooleanExpressionComparisonPair pair){
		ownPairList.add(pair);
	}
	public void addChildPair(BooleanExpressionComparisonPair pair){
		childPairList.add(pair);
	}
	public void addOwnPairList(List<BooleanExpressionComparisonPair> pairList){
		if(pairList!=null){
			for(BooleanExpressionComparisonPair ownPair: pairList){
				this.ownPairList.add(ownPair);
			}
		}
	}
	public void addChildResult(BooleanExpressionComparisonResult childResult){
		this.addChildPairList(childResult.getOwnPairList());
		this.addChildPairList(childResult.getChildPairList());
	}
	public void addChildPairList(List<BooleanExpressionComparisonPair> pairList){
		if(pairList!=null){
			for(BooleanExpressionComparisonPair childPair: pairList){
				this.childPairList.add(childPair);
			}
		}
	}
	@Override
	public String toString(){
		StringBuilder sb=new StringBuilder();
		sb.append("totalState=");
		sb.append(this.getTotalState().toString());
		sb.append(": ownPairList=");
		sb.append(ownPairList.toString());
		sb.append(", childPairList=");
		sb.append(childPairList.toString());
		return sb.toString();
	}
	
	public static class BooleanExpressionComparisonResultNone extends BooleanExpressionComparisonResult{
		private static final long serialVersionUID = 1L;
		static BooleanExpressionComparisonResultNone instance=null;
		private BooleanExpressionComparisonResultNone(){}
		public static BooleanExpressionComparisonResultNone getInstance(){
			if(instance==null){
				instance=new BooleanExpressionComparisonResultNone();
			}
			return instance;
		}
		@Override
		public State getOwnState(){
			return State.NONE;
		}
		@Override
		public State getChildrenState(){
			return State.NONE;
		}
		@Override
		public State getTotalState(){
			return State.NONE;
		}
	}
	
	/**
	public static class BooleanExpressionCompatibleComparisonResult extends BooleanExpressionComparisonResult{
		private static final long serialVersionUID = 1L;
		BooleanExpressionComparisonResult agentComparisonResult;
		public BooleanExpressionCompatibleComparisonResult(){
		}
		public BooleanExpressionCompatibleComparisonResult(BooleanExpressionComparisonResult ownCr, BooleanExpressionComparisonResult agentCr){
			this(ownCr);
			this.setAgentComparisonResult(agentCr);
		}
		public BooleanExpressionCompatibleComparisonResult(BooleanExpressionComparisonResult ownCr){
			super();
			this.addOwnPairList(ownCr.getOwnPairList());
			this.addChildPairList(ownCr.getChildPairList());
		}
		public BooleanExpressionComparisonResult getAgentComparisonResult() {
			return agentComparisonResult;
		}
		public void setAgentComparisonResult(BooleanExpressionComparisonResult agentCr) {
			this.agentComparisonResult = agentCr;
		}
	}
	*/
}
