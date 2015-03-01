package dist.esper.epl.expr;

public enum RelationTypeEnum {
	NONE("none"),
	AND("and"),
	OR("or"),
	NOT("not");
	
	private String string;
	private RelationTypeEnum(){
	}
	private RelationTypeEnum(String str){
		this.string=str;
	}
	
	@Override
	public String toString(){
		return string;
	}

	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}
}
