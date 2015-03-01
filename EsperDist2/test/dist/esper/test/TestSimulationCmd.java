package dist.esper.test;

import java.util.Scanner;

import dist.esper.external.Spout;

public class TestSimulationCmd extends TestSimulation {
	public static void main(String[] args){
		
	}
	
	@Override
	public void run(){
		for(Spout spout: spoutList){
			spout.start();
		}
		Scanner scanner = new Scanner(System.in);
		
		while(true){
			System.out.print(">>");
			String cmd=scanner.nextLine();
			if(cmd.equalsIgnoreCase("q")){
				break;
			}
			else{
				cmd=cmd.trim();
				if(cmd.endsWith(";")){
					cmd=cmd.substring(0, cmd.length()-1);
				}	
				try {
					coord.executeEPL(cmd);
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("bye!");
	}
}
