package dist.esper.test;

import org.apache.commons.configuration.*;
import org.apache.commons.configuration.plist.*;

public class TestPList {
	
	public static void main(String[] args){
		test1();
	}
	
	public static void test1(){
		PropertyListConfiguration config=new PropertyListConfiguration();
		try {
			config.load("./test.plist");
			Object array=config.getProperties("worker");
			System.out.print(array);
		}
		catch (ConfigurationException e) {			
			e.printStackTrace();
		}
	}
}
