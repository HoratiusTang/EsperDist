package dist.esper.epl.expr;


import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;

public class UnsolvedEventOrPropertyExpression2 extends AbstractIdentExpression{
	private static final long serialVersionUID = 6489877970128142878L;
	public static Pattern eventDotPropPattern = Pattern.compile("([^\\[\\]\\s]+)(\\[(\\d+)\\]){0,1}\\.([^\\[\\]\\s]+)(\\[(\\d+)\\]){0,1}$");
	public static Pattern eventOrPropPattern = Pattern.compile("([^\\[\\]\\s]+)(\\[(\\d+)\\]){0,1}$");
	String exprStr;
	
	public String eventAsName=null;
	public int eventIndex=-1;
	public String propName=null;
	public int propIndex=-1;
    
	public String eventAsNameOrPropName=null;
	public int eventIndexOrPropIndex=-1;
    
    boolean fullName=false;
	
    public UnsolvedEventOrPropertyExpression2(){}
    
	public UnsolvedEventOrPropertyExpression2(String exprStr) {
		super();
		this.exprStr = exprStr;
		preprocess();
	}
	
	public void preprocess(){
		if(exprStr.contains(".")){
			Matcher m=eventDotPropPattern.matcher(exprStr);
			if(m.matches()){
				eventAsName=m.group(1);
				propName=m.group(4);
				if(m.group(3)!=null){
					eventIndex=Integer.parseInt(m.group(3));
				}
				if(m.group(6)!=null){
					propIndex=Integer.parseInt(m.group(6));
				}
				fullName=true;
			}
		}
		else{
			Matcher m=eventOrPropPattern.matcher(exprStr);
			if(m.matches()){
				eventAsNameOrPropName=m.group(1);
				if(m.group(3)!=null){
					eventIndexOrPropIndex=Integer.parseInt(m.group(3));
				}
				fullName=false;
			}
		}
	}
	
	public boolean getFullName() {
		return fullName;
	}
	
	public boolean isFullName() {
		return fullName;
	}

	public void setFullName(boolean isFullName) {
		this.fullName = isFullName;
	}

	public String getExprStr() {
		return exprStr;
	}

	public void setExprStr(String exprStr) {
		this.exprStr = exprStr;
	}
	
	public String getEventAsName() {
		return eventAsName;
	}

	public void setEventAsName(String eventAsName) {
		this.eventAsName = eventAsName;
	}

	public int getEventIndex() {
		return eventIndex;
	}

	public void setEventIndex(int eventIndex) {
		this.eventIndex = eventIndex;
	}

	public String getPropName() {
		return propName;
	}

	public void setPropName(String propName) {
		this.propName = propName;
	}

	public int getPropIndex() {
		return propIndex;
	}

	public void setPropIndex(int propIndex) {
		this.propIndex = propIndex;
	}

	public String getEventAsNameOrPropName() {
		return eventAsNameOrPropName;
	}

	public void setEventAsNameOrPropName(String eventAsNameOrPropName) {
		this.eventAsNameOrPropName = eventAsNameOrPropName;
	}

	public int getEventIndexOrPropIndex() {
		return eventIndexOrPropIndex;
	}

	public void setEventIndexOrPropIndex(int eventIndexOrPropIndex) {
		this.eventIndexOrPropIndex = eventIndexOrPropIndex;
	}

	@Override
	public String toString(){
		return exprStr;
	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		sw.append(exprStr);
	}

	@Override
	public <T> T accept(IExpressionVisitor<T> visitor) {
		return null;
	}

	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj) {
		return null;
	}
}
