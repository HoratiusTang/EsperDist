package dist.esper.util;

public class ThreadUtil {
	public static void sleep(long timeMS){
		try{
			Thread.sleep(timeMS);
		}
		catch(Exception ex){
		}
	}
}
