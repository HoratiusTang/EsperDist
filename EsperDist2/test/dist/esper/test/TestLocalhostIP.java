package dist.esper.test;
import java.net.*;

public class TestLocalhostIP {
	public static void main(String[] args) throws Exception {
		String ip = InetAddress.getLocalHost().getHostAddress();
		System.out.println(ip);
    }
}
