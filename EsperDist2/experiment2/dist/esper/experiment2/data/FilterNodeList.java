package dist.esper.experiment2.data;

import java.util.*;

public class FilterNodeList extends AbstractNodeList<FilterNode> {
	FilterEventPropOpType filterType;
	
	public FilterNodeList(FilterEventPropOpType filterType) {
		super();
		this.filterType = filterType;
	}

	public FilterEventPropOpType getFilterType() {
		return filterType;
	}

	public void setFilterType(FilterEventPropOpType filterType) {
		this.filterType = filterType;
	}
}
