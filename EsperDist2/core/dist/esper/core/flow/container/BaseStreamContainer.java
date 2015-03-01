package dist.esper.core.flow.container;

import dist.esper.epl.expr.TimePeriod;
import dist.esper.epl.expr.ViewSpecification;

/**
 * the super-class of @FilterStreamContainer and @PatternStreamContainer.
 * it contains view specifications by users.
 * 
 * @see @BaseStream
 * @author tjy
 *
 */
public abstract class BaseStreamContainer extends
		DerivedStreamContainer {
	private static final long serialVersionUID = 1780155890509128737L;
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
			assert(viewSpecs[0].getNamespace().equals("win"));
			assert(viewSpecs[0].getFunction().equals("time"));
			TimePeriod t=(TimePeriod)viewSpecs[0].getParamList().get(0);
			long timeUS=t.getTimeUS();
			this.setWindowTimeUS(timeUS);
		}
	}
	@Override
	public int getLevel() {
		return 1;
	}
}
