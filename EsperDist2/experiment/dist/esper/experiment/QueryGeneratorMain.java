package dist.esper.experiment;

import java.util.*;

import dist.esper.experiment.QueryGenerator.IntPair;
import dist.esper.external.event.EventInstanceGenerator;

public class QueryGeneratorMain {
	public static void main(String[] args){
		QueryGenerator qg=new QueryGenerator();
		try {
			qg.readTemplatesFromFile("query/templates.txt");
			buildEvents(qg);
			List<IntPair> pairList=new ArrayList<IntPair>();
			pairList.add(new IntPair(2,2));
			List<String> queryList=qg.generateQuries(pairList);
			System.out.println(queryList.toString());
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
		
		qg.addEventName("A", "Ax");
		qg.addEventName("A", "Ay");
		
		qg.addEventName("B", "Bx");
		qg.addEventName("B", "By");
		
		qg.addEventName("C", "Cx");
		qg.addEventName("C", "Cy");
		
		qg.addEventName("D", "Dx");
		qg.addEventName("D", "Dy");
		
		qg.addEventName("E", "Ex");
		qg.addEventName("E", "Ey");
	}
}
