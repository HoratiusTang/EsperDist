package dist.esper.epl.expr;

public enum AggregationEnum {
	NONE("none"),
	AVEDEV("avedev"),
	AVG("avg"),
	COUNT("count"),
	COUNTSTAR("count"),
	MAX("max"),
	FMAX("fmax"),
	MEDIAN("median"),
	MIN("min"),
	FMIN("fmin"),
	STDDEV("stddev"),
	SUM("sum"),
	RATE("rate");
	
	private String name;
	private AggregationEnum(){
	}
	private AggregationEnum(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
