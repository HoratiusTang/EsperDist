package dist.esper.epl.expr;

import java.io.Serializable;

import java.util.ArrayList;

public abstract class ObjectSpecification implements Serializable{
	private static final long serialVersionUID = -9081055156690212159L;
	public String namespace=null;
	public String function=null;
	public ArrayList<AbstractResultExpression> paramList=null;
	
	public ObjectSpecification() {
		super();
	}

	public ObjectSpecification(String namespace, String function) {
		super();
		this.namespace = namespace;
		this.function = function;
		paramList=new ArrayList<AbstractResultExpression>(2);
	}

	public void addParameter(AbstractResultExpression param){
		paramList.add(param);
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}

	public ArrayList<AbstractResultExpression> getParamList() {
		return paramList;
	}
	
	public void setParamList(ArrayList<AbstractResultExpression> paramList) {
		this.paramList = paramList;
	}

	public abstract void toStringBuilder(StringBuilder sw);
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw);
		return sw.toString();
	}
}
