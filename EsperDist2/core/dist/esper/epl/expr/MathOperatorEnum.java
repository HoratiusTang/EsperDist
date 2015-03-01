package dist.esper.epl.expr;

public enum MathOperatorEnum {
	NONE(0,"none"),
	ADD(1,"+"),
	MINUS(1,"-"),
	MUL(2,"*"),
	DIV(2,"/"),
	MOD(2,"%");
	
	private String string;
	private int precedence;
	private MathOperatorEnum(){
	}
	private MathOperatorEnum(int precedence, String str) {
		this.string = str;
		this.precedence = precedence;
	}
	
	public void setString(String string) {
		this.string = string;
	}

	public void setPrecedence(int precedence) {
		this.precedence = precedence;
	}


	public String getString() {
		return string;
	}

	public int getPrecedence() {
		return precedence;
	}
	
	@Override
	public String toString() {
		return string;
	}
	
	public static class Factory{
		private static MathOperatorEnum[] ops=new MathOperatorEnum[]{
			MathOperatorEnum.ADD,
			MathOperatorEnum.MINUS,
			MathOperatorEnum.MUL,
			MathOperatorEnum.DIV,
			MathOperatorEnum.MOD
		};
		public static MathOperatorEnum valueOf(String str){
			for(MathOperatorEnum op: ops){
				if(op.getString().equals(str)){
					return op;
				}
			}
			return MathOperatorEnum.NONE;
		}
	}
}
