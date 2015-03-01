package dist.esper.epl.expr;


import com.espertech.esper.client.soda.TimePeriodExpression;
import com.espertech.esper.epl.expression.ExprNode;
import com.espertech.esper.epl.expression.ExprTimePeriod;

import dist.esper.epl.expr.util.IExpressionVisitor;
import dist.esper.epl.expr.util.IExpressionVisitor2;
import dist.esper.epl.sementic.StatementSementicWrapper;

//@JsonSerialize(using = ExpressionJsonSerializerFactory.TimePeriodJsonSerializer.class)
public class TimePeriod extends AbstractIdentExpression {
	private static final long serialVersionUID = -3869991533142761765L;
	public static final TimeUnitEnum[] TIME_UNITS={
		TimeUnitEnum.YEAR,
		TimeUnitEnum.MONTH,
		TimeUnitEnum.WEEK,
		TimeUnitEnum.DAY,
		TimeUnitEnum.HOUR,
		TimeUnitEnum.MINITE,
		TimeUnitEnum.SECOND,
		TimeUnitEnum.MILLISECOND
	};
	
	public static class Factory{
		public static TimePeriod make(ExprTimePeriod etp){
			ExprNode[] childNodes=etp.getChildNodes();
			long[] time=new long[TIME_UNITS.length];
			int i=0,j=0;
			if(etp.isHasYear()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			i++;
			if(etp.isHasMonth()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			i++;
			if(etp.isHasWeek()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			i++;
			if(etp.isHasDay()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			i++;
			if(etp.isHasHour()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			i++;
			if(etp.isHasMinute()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			i++;
			if(etp.isHasSecond()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			i++;
			if(etp.isHasMillisecond()){
				Value v=(Value)(ExpressionFactory.toExpression(childNodes[j++]));
				time[i]=v.intVal;
			}
			return new TimePeriod(time);
		}
		
		public static TimePeriod make(TimePeriodExpression tp0){
			//ExprNode[] childNodes=etp.getChildNodes();
			long[] time=new long[TIME_UNITS.length];
			int i=0,j=0;
			if(tp0.isHasYears()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			i++;
			if(tp0.isHasMonths()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			i++;
			if(tp0.isHasWeeks()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			i++;
			if(tp0.isHasDays()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			i++;
			if(tp0.isHasHours()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			i++;
			if(tp0.isHasMinutes()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			i++;
			if(tp0.isHasSeconds()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			i++;
			if(tp0.isHasMilliseconds()){
				Value v=(Value)(ExpressionFactory1.toExpression1(tp0.getChildren().get(j++)));
				time[i]=v.getIntVal();
			}
			return new TimePeriod(time);
		}
	}
	
	protected long[] time=new long[TIME_UNITS.length];
	
	public TimePeriod() {
		super();
	}

	public TimePeriod(long second){
		for(int i=0;i<time.length;i++){
			this.time[i]=0L;
		}
		this.time[time.length-2]=second;
	}
	
	public TimePeriod(long[] time) {
		super();
		for(int i=0;i<time.length;i++){
			this.time[i]=time[i];
		}		
	}

	public long[] getTime() {
		return time;
	}

	public void setTime(long[] time) {
		this.time = time;
	}

	public static TimeUnitEnum[] getTimeUnits() {
		return TIME_UNITS;
	}

	/**
	@Override
	public void toStringBuilder(StringBuilder sw) {
		int i=0;
		while(i<time.length && time[i]<=0){
			i++;
		}
		if(i==time.length){
			sw.append("0 second");
			return;
		}
		else{
			String delimiter="";
			for( ;i<time.length;i++){
				if(time[i]>0){
					sw.append(delimiter);
					sw.append(time[i]+" "+TIME_UNITS[i].toString());
					delimiter=" ";
				}
			}
		}
	}
	*/

	@Override
	public boolean resolve(StatementSementicWrapper ssw, Object param) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public int hashCode() {
		int base=439580927;
		for(long t: time){
			base ^= t;
		}
		return base;
	}
	
	public static int compare(TimePeriod a, TimePeriod b){
		//FIXME: not formalized
		for(int i=0; i<TIME_UNITS.length;i++){
			if(a.time[i]>b.time[i]){
				return 1;
			}
			else if(a.time[i]<b.time[i]){
				return -1;
			}
		}
		return 0;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof TimePeriod){
			TimePeriod that=(TimePeriod)obj;
			for(int i=0; i<TIME_UNITS.length;i++){
				if(this.time[i]!=that.time[i]){
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	public long getTimeUS(){
		//ATT: ignore week
		return ((((((time[0]*12+time[1])*30+time[3])*24+time[4])*60+time[5])*60+time[6])*1000+time[7])*1000;
	}

	@Override
	public <T> T accept(IExpressionVisitor<T> visitor){
		return visitor.visitTimePeriod(this);
	}
	
	@Override
	public <T, E> T accept(IExpressionVisitor2<T, E> visitor, E obj){
		return visitor.visitTimePeriod(this, obj);
	}
}
