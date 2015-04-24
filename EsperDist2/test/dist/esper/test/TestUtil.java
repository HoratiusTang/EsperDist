package dist.esper.test;

import dist.esper.core.comm.rawsocket.RawSocketLinkUtil;

public class TestUtil {
	public static void main(String[] args){
		test1();
	}
	
	public static void test1(){
		int a=12345678;
		byte[] b=RawSocketLinkUtil.toBytes(a);
		int c=RawSocketLinkUtil.toInt(b);
		System.out.println(a);
		System.out.println(c);
	}
}
