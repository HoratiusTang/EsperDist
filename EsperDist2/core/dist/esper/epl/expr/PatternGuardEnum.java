package dist.esper.epl.expr;

public enum PatternGuardEnum {
	TIMER_WITHIN("timer", "within"),
    TIMER_WITHINMAX("timer", "withinmax"),
    WHILE_GUARD("internal", "while");

    private String namespace;
    private String function;
    private PatternGuardEnum(){
    }
    private PatternGuardEnum(String namespace, String function){
    	this.namespace=namespace;
    	this.function=function;
    }

	public String getNamespace() {
		return namespace;
	}

	public String getFunction() {
		return function;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public void setFunction(String function) {
		this.function = function;
	}
}
