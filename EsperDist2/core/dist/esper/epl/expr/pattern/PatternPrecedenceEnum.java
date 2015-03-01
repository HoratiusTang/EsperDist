package dist.esper.epl.expr.pattern;

public enum PatternPrecedenceEnum {	
    MINIMUM(Integer.MIN_VALUE,"min"),
    FOLLOWEDBY(1,"->"),
    OR(2,"or"),
    AND(3,"and"),
    REPEAT_UNTIL(4,"[]"),
    //UNARY(5,"unary"),
    NOT(5,"not"),
    EVERY(6,"every"),
    FILTER(7,"filter"),
    GUARD_POSTFIX(8,"where"),
    ATOM(Integer.MAX_VALUE,"atom");
	
	private final int level;
	private final String str;
	
	private PatternPrecedenceEnum(int level, String str){
		this.level=level;
		this.str=str;
	}
	
	public int getLevel(){
		return level;
	}
	
	public String getString(){
		return str;
	}
	
	@Override
	public String toString(){
		return str;
	}
	
}
