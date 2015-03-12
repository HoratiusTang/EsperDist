package dist.esper.experiment;

import java.util.*;

import dist.esper.experiment.QueryGenerator.IntPair;
import dist.esper.experiment.util.MultiLineFileWriter;
import dist.esper.external.event.EventInstanceGenerator;

public class QueryGeneratorMain {
	public static void main(String[] args){
		QueryGenerator qg=new QueryGenerator();
		try {
			qg.readTemplatesFromFile("query/templates.txt");
			buildEvents(qg);
			
			IntPair[] pairs={
				new IntPair(1,20),
				new IntPair(2,10),
				new IntPair(3,5),
				new IntPair(4,2),
				new IntPair(5,1),
			};
			
			List<String> queryList=qg.generateQuries(Arrays.asList(pairs));
			//System.out.println(queryList.toString());
			MultiLineFileWriter.writeToFile("query/quries.txt", queryList);
		}
		catch (Exception e) {
			e.printStackTrace();			
		}
	}
	
	public static void buildEvents(QueryGenerator qg){
		EventInstanceGenerator eventGenA=EventGeneratorFactory.genEventInstanceGeneratorA();
		EventInstanceGenerator eventGenB=EventGeneratorFactory.genEventInstanceGeneratorB();
		EventInstanceGenerator eventGenC=EventGeneratorFactory.genEventInstanceGeneratorC();
		EventInstanceGenerator eventGenD=EventGeneratorFactory.genEventInstanceGeneratorD();
		EventInstanceGenerator eventGenE=EventGeneratorFactory.genEventInstanceGeneratorE();
		
		qg.addEventPrototype("A", eventGenA.getFieldGeneratorMap());
		qg.addEventPrototype("B", eventGenB.getFieldGeneratorMap());
		qg.addEventPrototype("C", eventGenC.getFieldGeneratorMap());
		qg.addEventPrototype("D", eventGenD.getFieldGeneratorMap());
		qg.addEventPrototype("E", eventGenE.getFieldGeneratorMap());
		
		qg.addEventName("A", "AJ");
		qg.addEventName("A", "AK");
		
		qg.addEventName("B", "BJ");
		qg.addEventName("B", "BK");
		
		qg.addEventName("C", "CJ");
		qg.addEventName("C", "CK");
		
		qg.addEventName("D", "DJ");
		qg.addEventName("D", "DK");
		
		qg.addEventName("E", "EJ");
		qg.addEventName("E", "EK");
	}
}
