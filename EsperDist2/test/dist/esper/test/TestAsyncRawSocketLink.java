package dist.esper.test;

import dist.esper.core.comm.rawsocket.async.AsyncRawSocketLink;

public class TestAsyncRawSocketLink {
	public static void main(String[] args){
		test1();
	}
	
	public static void test1(){
		AsyncRawSocketLink link=new AsyncRawSocketLink(null, null, null, 1024, 1024, 1024);
		String a="12345678";
		String b="abcdefghi";
		
		link.send(a);
		link.send(b);
	}
}
