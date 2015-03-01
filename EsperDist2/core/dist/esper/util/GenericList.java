package dist.esper.util;

import java.util.*;

public class GenericList extends ArrayList<Object>{
	private static final long serialVersionUID = 5733565909082957435L;

	@SuppressWarnings("unchecked")
	public <T> T getAt(int index){
		return (T)this.get(index);
	}
}
