package dist.esper.core.flow.stream;

import dist.esper.epl.expr.TimePeriod;
import dist.esper.epl.expr.ViewSpecification;

/**
 * the super-class of @FilterStream and @PatternStream.
 * it contains view specifications by users.
 * 
 * @see @BaseNode
 * @author tjy
 *
 */
public abstract class BaseStream extends DerivedStream{
	private static final long serialVersionUID = 5392770130251158345L;
	String optionalStreamName;
	ViewSpecification[] viewSpecs;
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
		if(viewSpecs!=null){
			assert(viewSpecs[0].getNamespace().equals("win")):viewSpecs[0].toString();
			assert(viewSpecs[0].getFunction().equals("time")):viewSpecs[0].toString();
			TimePeriod t=(TimePeriod)viewSpecs[0].getParamList().get(0);
			long timeUS=t.getTimeUS();
			this.setWindowTimeUS(timeUS);
		}
	}
	public int getLevel(){
		return 1;
	}
}
