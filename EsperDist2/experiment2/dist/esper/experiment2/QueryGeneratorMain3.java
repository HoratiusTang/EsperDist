package dist.esper.experiment2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dist.esper.core.util.NumberFormatter;
import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.experiment.util.MultiLineFileWriter;
import dist.esper.experiment2.data.NodesParameter;
import dist.esper.external.event.EventInstanceGenerator;
import dist.esper.util.ThreadUtil;

public class QueryGeneratorMain3 {
	static String[] eventNames={"A","B","C","D","E","F","G","H"};
	static EventInstanceGenerator[] eigs=EventGeneratorFactory2.genEventInstanceGenerators(eventNames);
	static OperatorTypeEnum[] filterOpTypes=new OperatorTypeEnum[]{
		OperatorTypeEnum.GREATER,
		OperatorTypeEnum.LESS
	};
	static OperatorTypeEnum[] joinOpTypes=new OperatorTypeEnum[]{
		OperatorTypeEnum.GREATER,
		OperatorTypeEnum.EQUAL,
		OperatorTypeEnum.LESS
	};
	static int[] windowTimes={60, 120, 180};
	static int numSelectElementsPerFilter=3;
	//static String filePathBase="query/query2";
	static double[] ratios={0.0, 0.25, 0.50, 0.75};
	static int[] filterOnlyqueryTotalCounts={150, 300, 450, 600};
	static int[] filterJoinqueryTotalCounts={100, 200, 300, 400};
	
	public static void main(String[] args){
		generateFilterOnlyQueryFiles();
		generateFilterJoinQueryFiles();
		//generateSingleFile();
	}
	
	public static boolean generateQueryFile(NodesParameter[] nodeParams, String filePathBase, 
			int queryTotalCount, double equalRatio, double implyRatio, boolean overwrite){
		String filePath=String.format("%s_%04d_%.2f_%.2f.txt", filePathBase, 
				queryTotalCount, equalRatio, implyRatio);
		File file=new File(filePath);
		if(!overwrite && file.exists()){
			//System.out.format("info: file '%s' already exists, will return\n", filePath);
			return true;
		}
		QueryGenerator2 queryGen2=new QueryGenerator2(
				eigs, filterOpTypes, joinOpTypes,
				windowTimes, numSelectElementsPerFilter,
				nodeParams
		);
		
		String headers=QueryGeneratorMain2.generateHeaders(eventNames, eigs, filterOpTypes, joinOpTypes,
				windowTimes, numSelectElementsPerFilter, nodeParams);
		System.out.format("info: begin generating %d queries into %s\n", queryTotalCount, filePath);
		try {
			List<String> queryList=queryGen2.generateQueries();
			List<String> headerAndQueryList=new ArrayList<String>(queryList.size()+2);
			headerAndQueryList.add(headers);
			headerAndQueryList.addAll(queryList);		
			MultiLineFileWriter.writeToFile(filePath, headerAndQueryList);
			System.out.format("info: finish generating %d queries into %s\n", queryList.size(), filePath);
			return true;
		}
		catch (Exception e){
			System.out.format("error: generate failed for %s: %s\n", filePath, e.getMessage());
			return false;
		}
	}
	
	public static void generateFilterOnlyQueryFiles(){
		String filePathBase="query/queries2_filter-only";
		
		NodesParameter[] nodeParams=new NodesParameter[]{
			new NodesParameter(1),
			new NodesParameter(2),
			new NodesParameter(3),
			new NodesParameter(4),
			new NodesParameter(5),
		};
		
		int successCount=0;
		int nodeCountPerType=25;
		nodeParams[0].nodeCountPerType=nodeCountPerType;
		for(int queryTotalCount: filterOnlyqueryTotalCounts){
			nodeParams[0].nodeCount=queryTotalCount;
			for(double equalRatio: ratios){
				for(double implyRatio: ratios){
					nodeParams[0].equalRatio=equalRatio;
					nodeParams[0].implyRatio=implyRatio;
					boolean result=generateQueryFile(nodeParams, filePathBase, queryTotalCount, equalRatio, implyRatio, false);
					if(result){
						successCount++;
					}
					ThreadUtil.sleep(200);
				}
			}
		}
		System.out.format("\ninfo: FINISH generating %d/%d filter-only query files.\n\n", successCount, 
				filterOnlyqueryTotalCounts.length * ratios.length * ratios.length);
	}
	
	public static void generateFilterJoinQueryFiles(){
		String filePathBase="query/queries2_filter-join";
		
		NodesParameter[] nodeParams=new NodesParameter[]{
			new NodesParameter(1),
			new NodesParameter(2),
			new NodesParameter(3),
			new NodesParameter(4),
			new NodesParameter(5),
		};
		
		int successCount=0;
		int nodeCountPerType=25;		
		for(int queryTotalCount: filterJoinqueryTotalCounts){
			nodeParams[0].nodeCount=queryTotalCount*40/100;
			nodeParams[1].nodeCount=queryTotalCount*30/100;
			nodeParams[2].nodeCount=queryTotalCount*15/100;
			nodeParams[3].nodeCount=queryTotalCount*10/100;
			nodeParams[4].nodeCount=queryTotalCount* 5/100;
			for(double equalRatio: ratios){
				for(double implyRatio: ratios){
					for(int i=0;i<nodeParams.length;i++){
						nodeParams[i].equalRatio=equalRatio;
						nodeParams[i].implyRatio=implyRatio;
						nodeParams[i].nodeCountPerType=nodeCountPerType;
						//adjust the nodeParams[i].nodeCountPerType, make it close to 25
						if(nodeParams[i].nodeCount % nodeParams[i].nodeCountPerType != 0){
							if(nodeParams[i].nodeCount > 2*nodeParams[i].nodeCountPerType){
								nodeParams[i].nodeCountPerType=20;
							}
							else{
								nodeParams[i].nodeCountPerType=nodeParams[i].nodeCount;
							}
						}						
					}
					boolean result=generateQueryFile(nodeParams, filePathBase, queryTotalCount, equalRatio, implyRatio, false);
					if(result){
						successCount++;
					}
					ThreadUtil.sleep(200);
				}
			}
		}
		System.out.format("\ninfo: FINISH generating %d/%d filter-join query files.\n\n", successCount, 
				filterJoinqueryTotalCounts.length * ratios.length * ratios.length);
	}
	
	/*
	public static void generateSingleFile(){
		String filePathBase="query/queries2_filter-only";
		
		NodesParameter[] nodeParams=new NodesParameter[]{
			new NodesParameter(1),
			new NodesParameter(2),
			new NodesParameter(3),
			new NodesParameter(4),
			new NodesParameter(5),
		};
		
		int nodeCountPerType=25;
		nodeParams[0].nodeCountPerType=nodeCountPerType;
		nodeParams[0].nodeCount=1500;
		nodeParams[0].equalRatio=0.75;
		nodeParams[0].implyRatio=0.75;
		generateQueryFile(nodeParams, filePathBase, nodeParams[0].nodeCount, 
				nodeParams[0].equalRatio, nodeParams[0].implyRatio, true);
	}
	*/
	public static List<EventInstanceGenerator> getEventInstanceGenerators(){
		return Arrays.asList(eigs);
	}
}
