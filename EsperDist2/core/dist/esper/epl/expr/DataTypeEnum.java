package dist.esper.epl.expr;

public enum DataTypeEnum{//FIXME: MORE TYPE
	NONE("none"),
	
	BYTE("byte"),
	CHAR("char"),
	SHORT("short"),
	INT("int"),
	LONG("long"),
	FLOAT("float"),
	DOUBLE("double"),
	BOOLEAN("boolean"),
	STRING("String"),
	
	BYTE_ARRAY("byte[]"),
	CHAR_ARRAY("char[]"),
	SHORT_ARRAY("short[]"),
	INT_ARRAY("int[]"),
	LONG_ARRAY("long[]"),
	FLOAT_ARRAY("float[]"),
	DOUBLE_ARRRAY("double[]"),
	BOOLEAN_ARRAY("boolean[]"),
	STRING_ARRAY("String[]");
	
	private String string;
	private DataTypeEnum(){
	}
	private DataTypeEnum(String str){
		this.string=str;
	}
	
	@Override
	public String toString(){
		return string;
	}
	
	public boolean isArray(){
		return string.endsWith("[]");
	}

	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}
	
}
