package dist.esper.epl.expr;


import java.util.*;

import com.espertech.esper.epl.declexpr.*;
import com.espertech.esper.epl.expression.*;
import com.espertech.esper.filter.*;
import com.espertech.esper.type.RelationalOpEnum;

import dist.esper.epl.sementic.StatementSementicWrapper;
import dist.esper.event.EventProperty;

@Deprecated
public class ExpressionFactory {
	public static AbstractExpression toExpression(ExprNode exprNode){
		if(exprNode instanceof ExprAggregateNode){
			ExprAggregateNode expr=(ExprAggregateNode)exprNode;
			return AggregationExpression.Factory.make(expr);
		}
		else if(exprNode instanceof ExprAndNode){
			ExprAndNode expr=(ExprAndNode)exprNode;
			ExprNode[] cns=expr.getChildNodes();
			CompositeExpression ce=new CompositeExpression(RelationTypeEnum.AND);
			for(ExprNode cn: cns){
				ce.addExpression((AbstractBooleanExpression)toExpression(cn));
			}
			return ce;
		}
		else if(exprNode instanceof ExprBetweenNode){
			
		}
		else if(exprNode instanceof ExprConstantNode){
			ExprConstantNode expr=(ExprConstantNode)exprNode;
			Value v=new Value(expr.getValue());
			return v;
		}
		else if(exprNode instanceof ExprDeclaredNode){
			
		}
		else if(exprNode instanceof ExprDotNode){
			ExprDotNode expr=(ExprDotNode)exprNode;
			UDFDotExpression ue=UDFDotExpression.Factory.make(expr);
			return ue;
		}
		else if(exprNode instanceof ExprEqualsNode){
			ExprEqualsNode expr=(ExprEqualsNode)exprNode;
			ExprNode[] cns=expr.getChildNodes();
			ComparisonExpression ce=new ComparisonExpression(
					expr.isNotEquals()?OperatorTypeEnum.NOT_EQUAL:OperatorTypeEnum.EQUAL);
			for(ExprNode cn: cns){
				ce.addExpression((AbstractResultExpression)toExpression(cn));
			}
			return ce;
		}
		else if(exprNode instanceof ExprIdentNode){
			ExprIdentNode expr=(ExprIdentNode)exprNode;
			String rsn=null, rpn=null;
			try{rsn=expr.getResolvedStreamName();}
			catch(Exception ex){}
			try{rpn=expr.getResolvedPropertyName();}
			catch(Exception ex){}
			UnsolvedEventOrPropertyExpression ue=
					new UnsolvedEventOrPropertyExpression(
							rsn,
							rpn,
							expr.getStreamOrPropertyName(),
							expr.getUnresolvedPropertyName()
							);
			return ue;
		}
		else if(exprNode instanceof ExprInNode){
			
		}
		else if(exprNode instanceof ExprMathNode){
			ExprMathNode expr=(ExprMathNode)exprNode;
			return MathExpression.Factory.make(expr);
		}
		else if(exprNode instanceof ExprOrNode){
			ExprOrNode expr=(ExprOrNode)exprNode;
			ExprNode[] cns=expr.getChildNodes();
			CompositeExpression ce=new CompositeExpression(RelationTypeEnum.OR);
			for(ExprNode cn: cns){
				ce.addExpression((AbstractBooleanExpression)toExpression(cn));
			}
			return ce;
		}
		else if(exprNode instanceof ExprRelationalOpNode){
			ExprRelationalOpNode expr=(ExprRelationalOpNode)exprNode;
			ExprNode[] cns=expr.getChildNodes();
			ComparisonExpression ce=new ComparisonExpression(toOperatorTypeEnum(expr.getRelationalOpEnum()));
			for(ExprNode cn: cns){
				ce.addExpression((AbstractResultExpression)toExpression(cn));
			}
			return ce;
		}
		else if(exprNode instanceof ExprStreamUnderlyingNode){
			
		}
		else if(exprNode instanceof ExprTimePeriod){
			ExprTimePeriod expr=(ExprTimePeriod)exprNode;
			TimePeriod tp=TimePeriod.Factory.make(expr);
			return tp;
		}
		System.err.format("unexpected ExprNode Type: %s\n", exprNode.getClass().toString());
		return null;
	}
	
	public static OperatorTypeEnum toOperatorTypeEnum(RelationalOpEnum ro){
		switch(ro){
		case GT:
			return OperatorTypeEnum.GREATER;
		case GE:
			return OperatorTypeEnum.GREATER_OR_EQUAL;
		case LT:
			return OperatorTypeEnum.LESS;
		case LE:
			return OperatorTypeEnum.LESS_OR_EQUAL;
		}
		return OperatorTypeEnum.NONE;
	}
	
	public static <E extends AbstractExpression> void toEPLParameterList(List<E> parameterList, StringBuilder sw){
		String delimiter="";
		for(AbstractExpression param: parameterList){
			sw.append(delimiter);
			delimiter=",";
			param.toStringBuilder(sw);
		}
	}
	
	public static AbstractPropertyExpression resolve(AbstractPropertyExpression expr, StatementSementicWrapper ssw, Object param) throws Exception{
		UnsolvedEventOrPropertyExpression uep=(UnsolvedEventOrPropertyExpression)expr;
		try{
//		if(expr instanceof UnsolvedEventOrPropertyExpression){
//			uep=(UnsolvedEventOrPropertyExpression)expr;
//			EventAlias eventAlias=null;
//			if(param instanceof EventAlias){
//				eventAlias=(EventAlias)param;
//			}
//			//if(uep.resolvedStreamName!=null){//EventTypeName
//			if(uep.unresolvedPropertyName==null){
//				EventAlias ea=null;
//				if(eventAlias!=null && 
//						(uep.resolvedStreamName.equals(eventAlias.event.getName()) || 
//						uep.resolvedStreamName.equals(eventAlias.event.getFullName()) ||
//							(eventAlias.getEventAsName()!=null && uep.resolvedStreamName.equals(eventAlias.getEventAsName())))) {
//					ea=eventAlias;
//				}
//				else{
//					ea=ssw.searchEventAlias(uep.resolvedStreamName);					
//				}
//				if(ea==null){
//					throw new Exception(String.format("can not find event with name %s", uep.resolvedStreamName));
//				}
//				EventSpecification es=new EventSpecification(ea);
//				if(uep.resolvedPropertyName!=null){
////					if(uep.resolvedPropertyName.startsWith(ea.eventAsName)){
////						uep.resolvedPropertyName=uep.resolvedPropertyName.substring(ea.eventAsName.length());
////					}
//					EventProperty prop=ea.event.getProperty(uep.resolvedPropertyName);
//					if(prop==null){
//						throw new Exception(String.format("can not find property %s in event %s", uep.resolvedPropertyName, uep.resolvedStreamName));
//					}
//					return new EventPropertySpecification(es,prop);
//				}
//				else{
//					return es;
//				}
//			}
//			else if(uep.streamOrPropertyName!=null){
//				EventAlias ea=ssw.eventAliasMap.get(uep.streamOrPropertyName);
//				if(ea==null){
//					throw new Exception(String.format("can not find event with name %s", uep.streamOrPropertyName));
//				}
//				EventSpecification es=new EventSpecification(ea);
//				if(uep.unresolvedPropertyName!=null){
//					EventProperty prop=ea.event.getProperty(uep.unresolvedPropertyName);
//					if(prop==null){
//						throw new Exception(String.format("can not find property %s in event %s", uep.unresolvedPropertyName, uep.streamOrPropertyName));
//					}
//					return new EventPropertySpecification(es,prop);
//				}
//				else{
//					return es;
//				}
//			}
//			else{//parse unresolvedPropertyName
//				String eventOrPropStr=null;
//				String indexStr=null;
//				String propStr=null;
//				EventSpecification es=null;
//				
//				int dotIndex=uep.unresolvedPropertyName.indexOf('.');
//				int leftBracketIndex=uep.unresolvedPropertyName.indexOf('[');
//				int rightBracketIndex=uep.unresolvedPropertyName.indexOf(']');
//				
//				if(dotIndex>0){
//					propStr=uep.unresolvedPropertyName.substring(dotIndex+1);
//				}
//				dotIndex=uep.unresolvedPropertyName.length();
//				
//				if(leftBracketIndex>0){
//					eventOrPropStr=uep.unresolvedPropertyName.substring(0,leftBracketIndex);
//					indexStr=uep.unresolvedPropertyName.substring(leftBracketIndex+1,rightBracketIndex);
//				}
//				else{
//					eventOrPropStr=uep.unresolvedPropertyName.substring(0, dotIndex);
//				}
//				
//				if(indexStr!=null || propStr!=null){
//					EventAlias ea=ssw.eventAliasMap.get(eventOrPropStr);
//					if(ea==null){
//						throw new Exception(String.format("can not find event with name %s", eventOrPropStr));
//					}
//					if(indexStr!=null){
//						int index=Integer.parseInt(indexStr);
//						es=new EventIndexedSpecification(ea,index);
//					}
//					else{
//						es=new EventSpecification(ea);
//					}
//					if(propStr!=null){
//						EventProperty prop=es.eventAlias.event.getProperty(propStr);
//						if(prop==null){
//							throw new Exception(String.format("can not find property %s in event %s", propStr, es.getEventAlias().getEvent().getFullName()));
//						}
//						return new EventPropertySpecification(es,prop);
//					}
//					return es;
//				}
//				else{
//					//eventOrPropStr is single event or property
//					EventAlias ea=ssw.searchProperty(eventOrPropStr);
//					if(ea!=null){//a property
//						es=new EventSpecification(ea);
//						EventProperty prop=ea.event.getProperty(eventOrPropStr);						
//						return new EventPropertySpecification(es,prop);
//					}
//					else{//a event
//						ea=ssw.eventAliasMap.get(eventOrPropStr);
//						if(ea==null){
//							throw new Exception(String.format("can not find event with name %s", eventOrPropStr));
//						}
//						es=new EventSpecification(ea);
//						return es;
//					}
//				}
//			}
//		}
		EventAlias relatedEventAlias=(EventAlias)param;  
		if(uep.isIdentified()){
			EventAlias ea=ssw.searchEventAlias(uep.eventAsName);
			if(ea==null){
				throw new Exception(String.format("can not find event with name %s", uep.eventAsName));
			}			
			EventSpecification es=new EventSpecification(ea, relatedEventAlias);
			if(uep.eventIndex>=0){
				es=new EventIndexedSpecification(ea, uep.eventIndex, relatedEventAlias);//is Array?
			}
			if(uep.propName==null){
				return es;
			}
			else{
				EventPropertySpecification eps=null;
				EventProperty prop=es.getEventAlias().getEvent().getProperty(uep.propName);
				if(prop==null){
					throw new Exception(String.format("can not find property %s in event %s", 
							uep.propName, es.getEventAlias().getEvent().getName()));
				}
				if(uep.propIndex<0){
					eps=new EventPropertySpecification(es, prop);
				}
				else{
					eps=new EventPropertyIndexedSpecification(es, prop, uep.propIndex);
				}
				return eps;
			}
		}
		else{
			EventAlias ea=ssw.searchEventAlias(uep.eventOrPropName);
			if(ea!=null){
				EventSpecification es=null;
				if(uep.eventOrPropIndex<0){
					es=new EventSpecification(ea, relatedEventAlias);
				}
				else{
					es=new EventIndexedSpecification(ea, uep.eventOrPropIndex, relatedEventAlias);
				}
				return es;
			}
			else{
				ea=ssw.searchProperty(uep.eventOrPropName);
				if(ea==null){
					throw new Exception(String.format("can not find event or property with name %s", uep.eventOrPropName));
				}
				EventSpecification es=new EventSpecification(ea, relatedEventAlias);
				EventProperty prop=es.getEventAlias().getEvent().getProperty(uep.eventOrPropName);
				
				EventPropertySpecification eps=null;
				if(uep.eventOrPropIndex<0){
					eps=new EventPropertySpecification(es, prop);
				}
				else{
					eps=new EventPropertyIndexedSpecification(es, prop, uep.eventOrPropIndex);
				}
				return eps;
			}
		}
		}
		catch(Exception ex){
			ex.printStackTrace();
			System.out.format("resolvedStreamName=%s, resolvedPropertyName=%s, streamOrPropertyName=%s, unresolvedPropertyName=%s, "+
					"identified=%s, eventAsName=%s, eventIndex=%d, propName=%s, propIndex=%d, "+
					"eventOrPropName=%s, eventOrPropIndex=%d\n",
					uep.resolvedStreamName,uep.resolvedPropertyName,uep.streamOrPropertyName,uep.unresolvedPropertyName,
					uep.identified, uep.eventAsName, uep.eventIndex, uep.propName, uep.propIndex,
					uep.eventOrPropName, uep.eventOrPropIndex);
			System.exit(0);
		}
		return expr;
		
	}
}
