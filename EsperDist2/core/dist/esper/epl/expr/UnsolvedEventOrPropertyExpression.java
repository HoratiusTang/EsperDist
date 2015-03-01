package dist.esper.epl.expr;


import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

public class UnsolvedEventOrPropertyExpression extends AbstractIdentExpression {
	private static final long serialVersionUID = -3446705869194982384L;
	public static Pattern eventDotPropPattern = Pattern.compile("([^\\[\\]\\s]+)(\\[(\\d+)\\]){0,1}\\.([^\\[\\]\\s]+)(\\[(\\d+)\\]){0,1}$");
	public static Pattern eventOrPropPattern = Pattern.compile("([^\\[\\]\\s]+)(\\[(\\d+)\\]){0,1}$");
	
	String unresolvedPropertyName=null;
    String streamOrPropertyName=null;

    String resolvedStreamName=null;
    String resolvedPropertyName=null;
    
    boolean identified=false;
    String eventAsName=null;
    int eventIndex=-1;
    String propName=null;
    int propIndex=-1;
    
    String eventOrPropName=null;
    int eventOrPropIndex=-1;
    
    public UnsolvedEventOrPropertyExpression(){
    	super();
    }
	public UnsolvedEventOrPropertyExpression(
			String resolvedStreamName,
			String resolvedPropertyName,
			String streamOrPropertyName, 
			String unresolvedPropertyName) {
		super();
		this.unresolvedPropertyName = unresolvedPropertyName;
		this.streamOrPropertyName = streamOrPropertyName;
		this.resolvedStreamName = resolvedStreamName;
		this.resolvedPropertyName = resolvedPropertyName;
		
//		System.out.format("resolvedStreamName=%s, resolvedPropertyName=%s, streamOrPropertyName=%s, unresolvedPropertyName=%s\n",
//				resolvedStreamName,resolvedPropertyName,streamOrPropertyName,unresolvedPropertyName);
		preprocess();
//		if(identified){
//			System.out.format("++ identified=%s, eventAsName=%s, eventIndex=%d, propName=%s, propIndex=%d\n",
//					identified, eventAsName, eventIndex, propName, propIndex);
//		}
//		else{
//			System.out.format("++ identified=%s, eventOrPropName=%s, eventOrPropIndex=%d\n",
//					identified, eventOrPropName, eventOrPropIndex);
//		}
	}
	
	public boolean isIdentified() {
		return identified;
	}
	
	public boolean getIdentified() {
		return identified;
	}

	public void preprocess(){
		if(resolvedStreamName!=null && resolvedPropertyName!=null){
			//resolvedStreamName can't be array
			eventAsName=resolvedStreamName;
			if(resolvedPropertyName.contains(".")){
				Matcher m=eventDotPropPattern.matcher(resolvedPropertyName);
				if(m.matches()){
					assert(eventAsName.equals(m.group(1)));
					propName=m.group(4);
					if(m.group(3)!=null){
						eventIndex=Integer.parseInt(m.group(3));
					}
					if(m.group(6)!=null){
						propIndex=Integer.parseInt(m.group(6));
					}
					identified=true;
				}
			}
			else{
				Matcher m=eventOrPropPattern.matcher(resolvedPropertyName);
				if(m.matches()){
					propName=m.group(1);
					if(m.group(3)!=null){
						propIndex=Integer.parseInt(m.group(3));
					}
					identified=true;
				}
			}
		}
		else{
			if(unresolvedPropertyName!=null && unresolvedPropertyName.contains(".")){
				Matcher m=eventDotPropPattern.matcher(unresolvedPropertyName);
				if(m.matches()){
					eventAsName=m.group(1);
					propName=m.group(4);
					if(m.group(3)!=null){
						eventIndex=Integer.parseInt(m.group(3));
					}
					if(m.group(6)!=null){
						propIndex=Integer.parseInt(m.group(6));
					}
					identified=true;
				}
			}
			else{
				if(streamOrPropertyName!=null){
					eventAsName=streamOrPropertyName;
//					if(unresolvedPropertyName!=null){
//						propName=unresolvedPropertyName;
//					}
					if(unresolvedPropertyName!=null){
						Matcher m=eventOrPropPattern.matcher(unresolvedPropertyName);
						if(m.matches()){
							propName=m.group(1);
							if(m.group(3)!=null){
								propIndex=Integer.parseInt(m.group(3));
							}							
						}
						else{
							propName=unresolvedPropertyName;
						}
					}
					identified=true;
				}
				else{
					Matcher m=eventOrPropPattern.matcher(unresolvedPropertyName);
					if(m.matches()){
						eventOrPropName=m.group(1);
						if(m.group(3)!=null){
							eventOrPropIndex=Integer.parseInt(m.group(3));
						}
						identified=false;//NOT solved
					}
				}
			}
		}
	}
	
//	@Override
//	public String toString(){
//		StringBuilder 
//		if(identified){
//			
//		}
//		else{
//			
//		}
//		if(resolvedStreamName!=null){
//			if(resolvedPropertyName!=null){
//				return resolvedStreamName+"."+resolvedPropertyName;
//			}
//			else{
//				return resolvedStreamName;
//			}
//		}
//		else if(streamOrPropertyName!=null){
//			if(unresolvedPropertyName!=null && !streamOrPropertyName.equals(unresolvedPropertyName)){
//				return streamOrPropertyName+"."+unresolvedPropertyName;
//			}
//			else{
//				return streamOrPropertyName;
//			}
//		}
//		else{
//			return unresolvedPropertyName;
//		}
//	}

	@Override
	public void toStringBuilder(StringBuilder sw) {
		if(identified){
			sw.append(eventAsName);
			if(eventIndex>=0){
				sw.append("["+eventIndex+"]");
			}
			sw.append(".");
			sw.append(propName);
			if(propIndex>=0){
				sw.append("["+propIndex+"]");
			}
		}
		else{
			sw.append(eventOrPropName);
			if(eventOrPropIndex>=0){
				sw.append("["+eventOrPropIndex+"]");
			}
		}
	}
	
	@Override
	public int hashCode(){
		return this.toString().hashCode();
	}

	@Override
	public boolean resolve(StatementSementicWrapper ssw,Object param) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public String getUnresolvedPropertyName() {
		return unresolvedPropertyName;
	}

	public void setUnresolvedPropertyName(String unresolvedPropertyName) {
		this.unresolvedPropertyName = unresolvedPropertyName;
	}

	public String getStreamOrPropertyName() {
		return streamOrPropertyName;
	}

	public void setStreamOrPropertyName(String streamOrPropertyName) {
		this.streamOrPropertyName = streamOrPropertyName;
	}

	public String getResolvedStreamName() {
		return resolvedStreamName;
	}

	public void setResolvedStreamName(String resolvedStreamName) {
		this.resolvedStreamName = resolvedStreamName;
	}

	public String getResolvedPropertyName() {
		return resolvedPropertyName;
	}

	public void setResolvedPropertyName(String resolvedPropertyName) {
		this.resolvedPropertyName = resolvedPropertyName;
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

	public String getEventOrPropName() {
		return eventOrPropName;
	}

	public void setEventOrPropName(String eventOrPropName) {
		this.eventOrPropName = eventOrPropName;
	}

	public int getEventOrPropIndex() {
		return eventOrPropIndex;
	}

	public void setEventOrPropIndex(int eventOrPropIndex) {
		this.eventOrPropIndex = eventOrPropIndex;
	}

	public void setIdentified(boolean identified) {
		this.identified = identified;
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
