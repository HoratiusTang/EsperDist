package dist.esper.epl.expr;



import dist.esper.epl.sementic.IResolvable;

public abstract class StreamSpecification extends AbstractClause {
	String optionalStreamName;
	ViewSpecification[] viewSpecs;
	
	public StreamSpecification() {
		super();
	}

	public String getOptionalStreamName() {
		return optionalStreamName;
	}

	public void setOptionalStreamName(String optionalStreamName) {
		this.optionalStreamName = optionalStreamName;
	}

	public ViewSpecification[] getViewSpecs() {
		return viewSpecs;
	}

	public void setViewSpecs(ViewSpecification[] viewSpecs) {
		this.viewSpecs = viewSpecs;
	}
	
	@Override
	public String toString(){
		StringBuilder sw=new StringBuilder();
		this.toStringBuilder(sw);
		return sw.toString();
	}
}
