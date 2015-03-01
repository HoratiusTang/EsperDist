package dist.esper.core.util;

import java.util.*;
import org.apache.commons.configuration.*;

import dist.esper.util.Logger2;
import dist.esper.util.OptionBuilder;

public class Config {
	static Logger2 log=Logger2.getLogger(Config.class);
	Map<String, String> cmdConfig=new HashMap<String, String>();
	FileConfiguration fileConfig=null;
	
	public Config() throws ConfigurationException{
		this(new String[]{});
	}
	public Config(String[] args) throws ConfigurationException{
		initDefaultOption(cmdConfig);
		OptionBuilder.parseArgs(args, cmdConfig);
		
		log.info("parse cmd options: "+cmdConfig.toString());
		
		String confFilePath=cmdConfig.get(Options.CONFIG_FILE_PATH);
		fileConfig=new PropertiesConfiguration();
		fileConfig.load(confFilePath);
	}
	
	public static void initDefaultOption(Map<String,String> context){
		context.put(Options.CONFIG_FILE_PATH, "./conf/config.properties");
		context.put(Options.LOG4J_CONF_PATH, "./conf/log4j.properties");
	}
	
	public String get(String key){
		String value=cmdConfig.get(key);
		if(value==null){
			value=(String)fileConfig.getProperty(key);
		}
		return value;
	}
}
