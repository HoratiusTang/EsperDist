package dist.esper.epl.expr;

public enum TimeUnitEnum {
	YEAR(0,"year"),
	MONTH(1,"month"),
	WEEK(2,"week"),
	DAY(3,"day"),
	HOUR(4,"hour"),
	MINITE(5,"minute"),
	SECOND(6,"second"),
	MILLISECOND(7,"millisecond");
	
	private int level;
	private String string;
	private TimeUnitEnum(){
	}	
	private TimeUnitEnum(int level, String str){
		this.level=level;
		this.string=str;
	}
	public int getLevel() {
		return level;
	}
	public String getString() {
		return string;
	}
	
	public void setLevel(int level) {
		this.level = level;
	}
	public void setString(String string) {
		this.string = string;
	}
	@Override
	public String toString() {
		return string;
	}
}
