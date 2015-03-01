package dist.esper.util;
import java.util.Arrays;

/**������λ�������� */
public class CarryIncrementPermutation {
	
	/**ȫ�����е�������� */
	public int MAX_NUM;
	
	/**��ǰ�н���:����ΪMAX_NUM+1.����curMediumNum[MAX_NUM]������λ,
	 * curMediumNum[2]������λ,curMediumNum[1]=0,curMediumNum[0]δ��
	 */
	public int[] curMediumNum;
	
	/**��ǰ����,����ΪMAX_NUM+1.����curPermutation[MAX_NUM]������λ,
	 * curPermutation[1]������λ,curPermutation[0]δ��
	 */
	public int[] curPermutation;
	
	/**�������*/
	public String name;
	
	/**������е��ַ����õ���StringBuilder*/
	public static StringBuilder sb=new StringBuilder();
	
	/**ʱ��ͳ��*/
	public long totalNextMediumTime=0;
	public int totalNextMediumCount=0;
	
	public long totalGenPermutaionTime=0;
	public long totalGenPermutaionTime2=0;
	public int totalGenPermutaionCount=0;
	
	public long totalGenMediumTime=0;
	public int totalGenMediumCount=0;
	
	/**
	 * ���캯��
	 * @param _MAX_NUM ȫ�����е��������
	 */
	public CarryIncrementPermutation(int _MAX_NUM){
		resetMaxNum(_MAX_NUM);
		name="������λ��ȫ�����㷨";
	}	
	
	/**
	 * �����н���
	 */
	protected void resetMediumNum(){
		Arrays.fill(curMediumNum, 0);
		curMediumNum[2]=-1;//��һ��ʹ��nextPermutationʹ�н�����1,ȫΪ0.
	}
	
	/**
	 * ���� MAX_NUM
	 * @param _MAX_NUM ȫ�����е��������
	 */
	public void resetMaxNum(int _MAX_NUM){
		MAX_NUM=_MAX_NUM;
		curMediumNum=new int[MAX_NUM+1];
		curPermutation=new int[MAX_NUM+1];
		
		Arrays.fill(curPermutation, 0);
		totalNextMediumTime=0;
		totalNextMediumCount=0;
		
		totalGenPermutaionTime=0;
		totalGenPermutaionTime2=0;
		totalGenPermutaionCount=0;
		
		resetMediumNum();
	}
	
	/**
	 * �����һ���н���
	 * @return ����н����Ѵ����ֵ,����false;�����н�����1,����true
	 */
	public int[] nextMediumNum(){
		long start=System.nanoTime();
		int carry=1;
		for(int num=2;num<=MAX_NUM;num++){
			curMediumNum[num]+=carry;
			carry=0;
			if(curMediumNum[num]>=num){
				curMediumNum[num]-=num;
				carry+=1;
			}
			else{
				break;
			}
		}
		if(carry>0){
			return null;
		}
		long end=System.nanoTime();
		totalNextMediumTime+=end-start;
		totalNextMediumCount++;
		return curMediumNum;
	}
	
	/**
	 * ������һ��ȫ����
	 * @return �������е�����,�±궨�����. 
	 */
	public int[] nextPermutation(){
		if(nextMediumNum()!=null){
			generatePermutationFromMediumNum(curMediumNum,curPermutation);
			return curPermutation;
		}
		else{
//			System.out.println("It's the last permutation.\n");
			return null;
		}
	}
	
	public int[] getCurPermutation() {
		return curPermutation;
	}

	public void setCurPermutation(int[] curPermutation) {
		this.curPermutation = curPermutation;
	}

	/**
	 * ��õ�ǰ���е��ַ������,��һ��ʹ��ǰ�����ȵ���nextPermutation().
	 * @return ��ǰ���е��ַ������
	 */
	public String curPermutationString(){	
		return getStringFromIntArray(curPermutation,true);
	}
	
	/**
	 * ��õ�ǰ�н������ַ������,��һ��ʹ��ǰ�����ȵ���nextPermutation().
	 * @return ��ǰ���е��ַ������
	 */
	public String curMediumNumString(){
		return getStringFromIntArray(curMediumNum,true);
	}
	
	
	/**
	 * �����н������ɶ�Ӧ������,ʹ�����ַ����ֱ�ʵ��
	 * @param mediumNum �н���
	 * @param permutation ������е�����
	 */
	public void generatePermutationFromMediumNum(int[] mediumNum,int[] permutation){
		long start,end;		
		int maxNum,spaceCount,num,j,pre,temp;
		int[] next;
		
		//------------------����Ϊδ�Ż�����----------------//
		start=System.nanoTime();		
		maxNum=mediumNum.length-1;
		spaceCount=0;
		
		Arrays.fill(permutation, 0);
		for(num=maxNum;num>=1;num--){
			spaceCount=0;
			for(j=1;j<=maxNum;j++){				
				if(permutation[j]==0){
					spaceCount++;
					temp=mediumNum[num]+1;
					if(spaceCount==temp){
						permutation[j]=num;
						break;
					}
				}
			}
		}
		end=System.nanoTime();
		totalGenPermutaionTime += end-start;
		
		//------------------����Ϊ�Ż�����----------------//		
//		start=System.nanoTime();
//		next=new int[maxNum+1];		
//		for(j=0;j<=maxNum;j++){//0��ͷ���
//			next[j]=j+1;
//		}
//		for(num=maxNum;num>=1;num--){
//			j=0;
//			pre=0;
//			spaceCount=0;
//			temp=mediumNum[num]+1;
//			while(spaceCount<temp){
//				pre=j;
//				j=next[j];
//				spaceCount++;				
//			}
//			permutation[j]=num;
//			next[pre]=next[j];
//		}
//		
//		end=System.nanoTime();
//		totalGenPermutaionTime2 += end-start;
		totalGenPermutaionCount++;	
	}
	
	/**
	 * �����������ɶ�Ӧ���н���
	 * @param permutation ����
	 * @param mediumNum ����н���������
	 */
	public void generateMediumNumFromPermutation(int[] permutation,int[] mediumNum){
		long start=System.nanoTime();
		int maxNum=permutation.length-1;
		int num;
		Arrays.fill(mediumNum, 0);
		for(int i=maxNum;i>=1;i--){
			num=permutation[i];
			for(int j=i-1;j>=1;j--){
				if(num>permutation[j]){
					mediumNum[num]++;
				}
			}
		}
		long end=System.nanoTime();
		totalGenMediumTime += end-start;
		totalGenMediumCount++;
	}
	
	/**
	 * ͨ�þ�̬����,��int���鵹��װ��Ϊ�ַ���. 
	 * @param array ��������
	 * @param highToHighΪtrue��ʾ���ִ��ĸ�λ���±�ϴ�,��֮��Ȼ.
	 * @return �ַ���
	 */
	public static String getStringFromIntArray(int array[],boolean highToHigh){
		sb.delete(0, sb.length());
		if(highToHigh){
			for(int num=array.length-1;num>=1;num--){
				sb.append(array[num]);
			}
		}
		else{
			for(int num=1;num<=array.length-1;num++){
				sb.append(array[num]);
			}
		}
		return sb.toString();
	}
}