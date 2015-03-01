package dist.esper.test;

import org.hyperic.sigar.*;
import org.hyperic.sigar.cmd.Netstat;

public class TestSigar {
	public static void main(String[] args){
		//Sigar sigar=new Sigar();
		//System.out.println(getCpuCount());
		//getCpuTotal();
		test1();
	}
	
	public static void test1(){
		Sigar sigar = new Sigar();
		Cpu cpu;
		Mem mem;
		CpuPerc cpp;
		try {
			cpu = sigar.getCpu();
			cpp=sigar.getCpuPerc();
//			for(int i=0; i<Integer.MAX_VALUE;i++){
//				cpu.gather(sigar);
//				cpp=sigar.getCpuPerc();
//				
//				//double totalP=cpp.getTotal();
//				double idleP=cpp.getIdle();
//				double userP=cpp.getUser();
//				double sysP=cpp.getSys();
//				double waitP=cpp.getWait();
//				double stolenP=cpp.getStolen();
//				double niceP=cpp.getNice();
//				double irqP=cpp.getIrq();
//				double softIrqP=cpp.getSoftIrq();
//				
//				System.out.println("-----------------------------");
//				//System.out.println(totalP+" [total]");
//				System.out.println(sysP+" [sys]");
//				System.out.println(userP+" [user]"); 
//				System.out.println(idleP+" [idle]");				
//				System.out.println(waitP+" [wait]");
//				System.out.println(niceP+" [nice]");
//				System.out.println(stolenP+" [stolen]");
//				System.out.println(irqP+" [irq]");
//				System.out.println(softIrqP+" [softIrq]");
//				System.out.println();
//				long total=cpu.getTotal();
//				long idle=cpu.getIdle();
//				long user=cpu.getUser();
//				long sys=cpu.getSys();
//				long wait=cpu.getWait();
//				long stolen=cpu.getStolen();
//				long nice=cpu.getNice();
//				long irq=cpu.getIrq();
//				long softIrq=cpu.getSoftIrq();
//				System.out.println(total+" [total]");
//				System.out.println(sys+" [sys]");
//				System.out.println(user+" [user]"); 
//				System.out.println(idle+" [idle]");				
//				System.out.println(wait+" [wait]");
//				System.out.println(nice+" [nice]");
//				System.out.println(stolen+" [stolen]");
//				System.out.println(irq+" [irq]");
//				System.out.println(softIrq+" [softIrq]");
//				
////				System.out.println((double)user/total);
////				System.out.println((double)idle/total);
//				System.out.println();
//				Thread.sleep(500);
//			}
			mem=sigar.getMem();
//			for(int i=0; i<Integer.MAX_VALUE;i++){
//				mem.gather(sigar);
//				long total=mem.getTotal();
//				long used=mem.getUsed();
//				long free=mem.getFree();
//				long actUsed=mem.getActualUsed();
//				long actFree=mem.getActualFree();
//				double usePect=mem.getUsedPercent();
//				double freePect=mem.getFreePercent();
//				
//				System.out.format("total=%d, used=%d, actUsed=%d, free=%d, actFree=%d, usePercent=%.2f, freePercent=%.2f\n",
//						total, used, actUsed, free, actFree, usePect, freePect);
//				
//				Thread.sleep(500);
//			}
			NetStat ns=null;
			for(int i=0; i<Integer.MAX_VALUE;i++){
				ns=sigar.getNetStat();
				int idle=ns.getTcpIdle();
				int inBound=ns.getTcpInboundTotal();
				int outBound=ns.getTcpOutboundTotal();
				
				System.out.println(idle);
				System.out.println(inBound);
				System.out.println(outBound);
				System.out.println();
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static int getCpuCount(){
        try {
        	Sigar sigar = new Sigar(); 
            int cpuCount=sigar.getCpuInfoList().length;
            sigar.close();
            return cpuCount;
        }
        catch(SigarException e){
        	e.printStackTrace();
        	return 0;
        }  
    }
	public static void getCpuTotal() {   
        Sigar sigar = new Sigar();
        
        CpuInfo[] infos;   
        try {   
            infos = sigar.getCpuInfoList();   
            for (int i = 0; i < infos.length; i++) {// �����ǵ���CPU���Ƕ�CPU������  
                CpuInfo info = infos[i];   
                System.out.println("CPU������:" + info.getMhz());// CPU������MHz  
                System.out.println("���CPU��������" + info.getVendor());// ���CPU���������磺Intel  
                System.out.println("CPU�����" + info.getModel());// ���CPU������磺Celeron  
                System.out.println("����洢��������" + info.getCacheSize());// ����洢������  
                System.out.println("**************");   
            }   
        } catch (SigarException e) {   
            e.printStackTrace();   
        }   
    }   
}
