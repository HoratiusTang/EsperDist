package dist.esper.core.util;

import java.util.*;
import org.apache.commons.configuration.*;

import dist.esper.util.Logger2;
import dist.esper.util.OptionBuilder;

public class Config {
	static Logger2 log=Logger2.getLogger(Config.class);
	/* priority: cmdConfig > fileConfig > defualtConfig */
	Map<String, String> cmdConfig=new HashMap<String, String>();
	FileConfiguration fileConfig=null;
	Map<String, String> defualtConfig=new HashMap<String, String>();
	
	
	public Config() throws ConfigurationException{
		this(new String[]{});
	}
	public Config(String[] args) throws ConfigurationException{
		initDefaultOption(defualtConfig);
		OptionBuilder.parseArgs(args, cmdConfig);
		
		log.info("parse cmd options: "+cmdConfig.toString());
		
		String confFilePath=this.get(Options.CONFIG_FILE_PATH);
		fileConfig=new PropertiesConfiguration();
		fileConfig.load(confFilePath);
	}
	
	public static void initDefaultOption(Map<String,String> context){
		context.put(Options.CONFIG_FILE_PATH, "./conf/config.properties");
		context.put(Options.LOG4J_CONF_PATH, "./conf/log4j.properties");
		context.put(Options.LOG_QUERY_RESULT, "false");
		context.put(Options.WORKER_NUMBER_OF_PROCESS_THREADS, "2");
		context.put(Options.WORKER_NUMBER_OF_PUBLISH_THREADS, "1");
		context.put(Options.KRYONET_WRITE_BUFFER_SIZE, "4096000");
		context.put(Options.KRYONET_OBJECT_BUFFER_SIZE, "2048000");
	}
	
	public String get(String key){
		String value=cmdConfig.get(key);
		if(value==null){
			if(fileConfig!=null){
				value=(String)fileConfig.getProperty(key);
			}
			if(value==null){
				value=defualtConfig.get(key);
			}
		}
		return value;
	}
	
	public long getLong(String key, long defaultLong){
		String v=get(key);
		if(v==null){
			return defaultLong;
		}
		try{
			long n=Long.parseLong(v);
			return n;
		}
		catch(Exception ex){
			return defaultLong;
		}
	}
	
	public double getDouble(String key, int defaultDouble){
		String v=get(key);
		if(v==null){
			return defaultDouble;
		}
		try{
			double d=Double.parseDouble(v);
			return d;
		}
		catch(Exception ex){
			return defaultDouble;
		}
	}
	
	public String getString(String key, String defaultString){
		String v=get(key);
		if(v==null){
			return defaultString;
		}
		return v;
	}
}
