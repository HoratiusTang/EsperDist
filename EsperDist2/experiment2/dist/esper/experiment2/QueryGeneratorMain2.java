package dist.esper.experiment2;

import java.util.List;

import dist.esper.epl.expr.OperatorTypeEnum;
import dist.esper.experiment.util.MultiLineFileWriter;
import dist.esper.experiment2.data.NodesParameter;
import dist.esper.external.event.EventInstanceGenerator;

public class QueryGeneratorMain2 {
	public static void main(String[] args){
		String[] eventNames={"A","B","C","D","E","F","G","H","R","T"};
		EventInstanceGenerator[] eigs=EventGeneratorFactory2.genEventInstanceGenerators(eventNames);
		OperatorTypeEnum[] filterOpTypes=new OperatorTypeEnum[]{
			OperatorTypeEnum.GREATER,
			OperatorTypeEnum.LESS
		};
		OperatorTypeEnum[] joinOpTypes=new OperatorTypeEnum[]{
			OperatorTypeEnum.GREATER,
			OperatorTypeEnum.EQUAL,
			OperatorTypeEnum.LESS
		};
		int[] windowTimes={60, 120, 180};
		int numSelectElementsPerFilter=3;
		
		String filePath="query/queries2.txt";
		NodesParameter[] nodeParams=new NodesParameter[]{
			new NodesParameter(1, 60, 15, 0.3, 0.2),
			new NodesParameter(2, 36, 12, 0.3, 0.3),
			new NodesParameter(3, 24, 8, 0.3, 0.3),
			new NodesParameter(4, 10, 5, 0.3, 0.3),
			new NodesParameter(5, 10, 5, 0.3, 0.3),
		};
		QueryGenerator2 queryGen2=new QueryGenerator2(
			eigs, filterOpTypes, joinOpTypes,
			windowTimes, numSelectElementsPerFilter,
			nodeParams
		);
		
		List<String> queryList=queryGen2.generateQueries();
		try {
			MultiLineFileWriter.writeToFile(filePath, queryList);
		}
		catch (Exception e) {
			e.printStackTrace();
		}		
		for(String query: queryList){
			System.out.println(query);
			System.out.println();
		}
	}
}
