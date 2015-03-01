package dist.esper.epl.expr;

public enum OperatorTypeEnum {
	EQUAL("="),
    //IS("is"),
    
    //IS_NOT("is not"),

    LESS("<"),

    LESS_OR_EQUAL("<="),

    GREATER_OR_EQUAL(">="),

    GREATER(">"),
	
	NOT_EQUAL("!="),
	
	NONE("none");

//    RANGE_OPEN("(,)"),
//
//    RANGE_CLOSED("[,]"),
//
//    RANGE_HALF_OPEN("[,)"),
//    
//    RANGE_HALF_CLOSED("(,]"),
//    
//    NOT_RANGE_OPEN("-(,)"),
//
//    NOT_RANGE_CLOSED("-[,]"),
//
//    NOT_RANGE_HALF_OPEN("-[,)"),
//    
//    NOT_RANGE_HALF_CLOSED("-(,]"),
//    
//    IN_LIST_OF_VALUES("in"),
//
//    NOT_IN_LIST_OF_VALUES("!in"),
//
//    BOOLEAN_EXPRESSION("boolean_expr");
	
	private String string;
	private OperatorTypeEnum(){
	}
	private OperatorTypeEnum(String str){
		this.string=str;
	}
	
	@Override
	public String toString(){
		return string;
	}
	
	public OperatorTypeEnum opposite(){
		switch(this){
		case EQUAL:
		case NOT_EQUAL:
			return this;
		case LESS:
			return GREATER_OR_EQUAL;
		case GREATER:
			return LESS_OR_EQUAL;
		case LESS_OR_EQUAL:
			return GREATER;
		case GREATER_OR_EQUAL:
			return LESS;
		}
		return OperatorTypeEnum.NONE;
	}
	
	public OperatorTypeEnum reverse(){
		switch(this){
		case EQUAL:
		case NOT_EQUAL:
			return this;
		case LESS:
			return GREATER;
		case GREATER:
			return LESS;
		case LESS_OR_EQUAL:
			return GREATER_OR_EQUAL;
		case GREATER_OR_EQUAL:
			return LESS_OR_EQUAL;
		}
		return OperatorTypeEnum.NONE;
	}

	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}
	
	public static class Factory{
		private static OperatorTypeEnum[] types={
			OperatorTypeEnum.EQUAL,
			OperatorTypeEnum.LESS,
			OperatorTypeEnum.LESS_OR_EQUAL,
			OperatorTypeEnum.GREATER,
			OperatorTypeEnum.GREATER_OR_EQUAL,
			OperatorTypeEnum.NOT_EQUAL
		};
		public static OperatorTypeEnum valueOf(String str){
			for(OperatorTypeEnum type: types){
				if(type.getString().equals(str)){
					return type;
				}
			}
			return OperatorTypeEnum.NONE;
		}
	}
}
