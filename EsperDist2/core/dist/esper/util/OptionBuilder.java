package dist.esper.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class OptionBuilder {
	public static Logger logger = Logger.getLogger(OptionBuilder.class);
	
	public static String CMD_OPTIION_PREFIX="-";
	
	private static String removeCmdOptionPrefix(String arg){
		if(arg.startsWith(CMD_OPTIION_PREFIX)){
			return arg.substring(CMD_OPTIION_PREFIX.length());
		}
		return arg;
	}
	
	public static List<String> parseArgs(String[] args, Map<String,String> context){
		List<String> remainArgList=new ArrayList<String>();
		int index=0;
		while(index<args.length){
			if(args[index].startsWith(CMD_OPTIION_PREFIX)){
				if(index+1 < args.length && !args[index+1].startsWith(CMD_OPTIION_PREFIX)){
					context.put(removeCmdOptionPrefix(args[index]), args[index+1]);
					index++;
				}
				else{
					context.put(removeCmdOptionPrefix(args[index]), "");
				}
			}
			else{
				remainArgList.add(removeCmdOptionPrefix(args[index]));
			}
			index++;
		}
		return remainArgList;
	}
	
	public static String convertEscapeChar(String delimiter){
		delimiter=delimiter.replaceAll("\\\\r", "\r").replaceAll("\\\\n", "\n").replaceAll("\\\\t", "\t");
		return delimiter;
	}
}
