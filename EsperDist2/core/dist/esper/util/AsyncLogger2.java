package dist.esper.util;

import org.apache.log4j.*;

/**
 * an async logger
 * @author tjy
 * @see http://liuna718-163-com.iteye.com/blog/1900151
 * @see http://www.itcast.cn/news/20130814/09430685598.shtml
 */
public class AsyncLogger2 {	
	public static Logger2 getAsyncLogger(Class<?> clazz, Level level, 
			String filePath, boolean isAppend, String pattern){
		Logger log = Logger.getLogger(clazz);
		//log.setAdditivity(false); //ATT: DO NOT inherit from parent logger
		//log.removeAllAppenders();
		log.setLevel(level);
		
		
		FileAppender fileAppender = new DailyRollingFileAppender();  
        PatternLayout layout = new PatternLayout();        
        layout.setConversionPattern(pattern);
        fileAppender.setLayout(layout);
        fileAppender.setFile(filePath);
        fileAppender.setAppend(isAppend);
//        fileAppender.setBufferedIO(true);
        fileAppender.activateOptions();
        //fileAppender.set
        
//        AsyncAppender asyncAppender=new AsyncAppender();
//        asyncAppender.addAppender(fileAppender);
//        log.addAppender(asyncAppender);
        
        log.addAppender(fileAppender);
		return new Logger2(log);
	}
}
