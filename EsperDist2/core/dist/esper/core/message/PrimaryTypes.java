package dist.esper.core.message;

import java.io.Serializable;

public class PrimaryTypes implements Serializable {
	private static final long serialVersionUID = 2031751732063495002L;
	public static final int SUBSCRIBE=1;
	public static final int CONTROLL=2;
	public static final int RESPONSE=3;
	public static final int DATA=4;
}
