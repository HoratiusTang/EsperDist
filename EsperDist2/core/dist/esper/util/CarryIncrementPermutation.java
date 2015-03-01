package dist.esper.util;
import java.util.Arrays;

/**递增进位制排列类 */
public class CarryIncrementPermutation {
	
	/**全排列中的最大数字 */
	public int MAX_NUM;
	
	/**当前中介数:长度为MAX_NUM+1.其中curMediumNum[MAX_NUM]存放最高位,
	 * curMediumNum[2]存放最低位,curMediumNum[1]=0,curMediumNum[0]未用
	 */
	public int[] curMediumNum;
	
	/**当前排列,长度为MAX_NUM+1.其中curPermutation[MAX_NUM]存放最高位,
	 * curPermutation[1]存放最低位,curPermutation[0]未用
	 */
	public int[] curPermutation;
	
	/**类的名字*/
	public String name;
	
	/**输出排列的字符串用到的StringBuilder*/
	public static StringBuilder sb=new StringBuilder();
	
	/**时间统计*/
	public long totalNextMediumTime=0;
	public int totalNextMediumCount=0;
	
	public long totalGenPermutaionTime=0;
	public long totalGenPermutaionTime2=0;
	public int totalGenPermutaionCount=0;
	
	public long totalGenMediumTime=0;
	public int totalGenMediumCount=0;
	
	/**
	 * 构造函数
	 * @param _MAX_NUM 全排列中的最大数字
	 */
	public CarryIncrementPermutation(int _MAX_NUM){
		resetMaxNum(_MAX_NUM);
		name="递增进位制全排列算法";
	}	
	
	/**
	 * 重设中介数
	 */
	protected void resetMediumNum(){
		Arrays.fill(curMediumNum, 0);
		curMediumNum[2]=-1;//第一次使用nextPermutation使中介数加1,全为0.
	}
	
	/**
	 * 重设 MAX_NUM
	 * @param _MAX_NUM 全排列中的最大数字
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
	 * 获得下一个中介数
	 * @return 如果中介数已达最大值,返回false;否则中介数加1,返回true
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
	 * 生成下一个全排列
	 * @return 返回排列的数组,下标定义见上. 
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
	 * 获得当前排列的字符串输出,第一次使用前必须先调用nextPermutation().
	 * @return 当前排列的字符串输出
	 */
	public String curPermutationString(){	
		return getStringFromIntArray(curPermutation,true);
	}
	
	/**
	 * 获得当前中介数的字符串输出,第一次使用前必须先调用nextPermutation().
	 * @return 当前排列的字符串输出
	 */
	public String curMediumNumString(){
		return getStringFromIntArray(curMediumNum,true);
	}
	
	
	/**
	 * 根据中介数生成对应的排列,使用两种方法分别实现
	 * @param mediumNum 中介数
	 * @param permutation 存放排列的数组
	 */
	public void generatePermutationFromMediumNum(int[] mediumNum,int[] permutation){
		long start,end;		
		int maxNum,spaceCount,num,j,pre,temp;
		int[] next;
		
		//------------------以下为未优化方法----------------//
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
		
		//------------------以下为优化方法----------------//		
//		start=System.nanoTime();
//		next=new int[maxNum+1];		
//		for(j=0;j<=maxNum;j++){//0是头结点
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
	 * 根据排列生成对应的中介数
	 * @param permutation 排列
	 * @param mediumNum 存放中介数的数组
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
	 * 通用静态方法,将int数组倒序装换为字符串. 
	 * @param array 整型数组
	 * @param highToHigh为true表示数字串的高位的下标较大,反之亦然.
	 * @return 字符串
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