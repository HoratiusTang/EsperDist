package dist.esper.util;

import org.apache.log4j.*;

/**
 * a custom logger supporting formatting like System.out.format(format, args...), 
 * so user can invoke methods like debug("%s is running with %d %s\n", "Master", 5, "Slaves"), 
 * most of the code is copied from Log4JLogger.java commons-logging-1.1.3.jar.
 * 
 * @author tjy
 *
 */
public class Logger2 {
	private static final String FQCN = Logger2.class.getName();
	private static final Priority traceLevel;
	static {
		if (!Priority.class.isAssignableFrom(Level.class)) {
			throw new InstantiationError("Log4J 1.2 not available");
		}
		Priority _traceLevel;
		try {
			_traceLevel = (Priority) Level.class.getDeclaredField("TRACE").get(
					null);
		} catch (Exception ex) {
			_traceLevel = Level.DEBUG;
		}
		traceLevel = _traceLevel;
	}

	Logger logger;

	protected Logger2(Logger logger) {
		this.logger = logger;
	}

	public static Logger2 getLogger(String name) {
		Logger logger=LogManager.getLogger(name);
		assert(logger!=null);
		Logger2 logger2=new Logger2(logger);
		return logger2;
	}

	public static Logger2 getLogger(Class<?> clazz) {
		Logger logger=LogManager.getLogger(clazz);
		assert(logger!=null);
		Logger2 logger2=new Logger2(logger);
		return logger2;
	}

	public Logger getLogger() {
		return logger;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	public String getName(){
		return getLogger().getName();
	}

	public void trace(Object message) {
		getLogger().log(FQCN, traceLevel, message, null);
	}

	public void trace(Object message, Throwable t) {
		getLogger().log(FQCN, traceLevel, message, t);
	}
	
	public void trace(String format, Object... args) {
		String message = String.format(format, args);
		getLogger().log(FQCN, traceLevel, message, null);
	}

	public void debug(Object message) {
		getLogger().log(FQCN, Level.DEBUG, message, null);
	}

	public void debug(Object message, Throwable t) {
		getLogger().log(FQCN, Level.DEBUG, message, t);
	}
	
	public void debug(String format, Object... args) {
		String message = String.format(format, args);
		getLogger().log(FQCN, Level.DEBUG, message, null);
	}

	public void info(Object message) {
		getLogger().log(FQCN, Level.INFO, message, null);
	}

	public void info(Object message, Throwable t) {
		getLogger().log(FQCN, Level.INFO, message, t);
	}
	
	public void info(String format, Object... args) {
		String message = String.format(format, args);
		getLogger().log(FQCN, Level.INFO, message, null);
	}

	public void warn(Object message) {
		getLogger().log(FQCN, Level.WARN, message, null);
	}

	public void warn(Object message, Throwable t) {
		getLogger().log(FQCN, Level.WARN, message, t);
	}
	
	public void warn(String format, Object... args) {
		String message = String.format(format, args);
		getLogger().log(FQCN, Level.WARN, message, null);
	}

	public void error(Object message) {
		getLogger().log(FQCN, Level.ERROR, message, null);
	}

	public void error(Object message, Throwable t) {
		getLogger().log(FQCN, Level.ERROR, message, t);
	}
	
	public void error(String format, Object... args) {
		String message = String.format(format, args);
		getLogger().log(FQCN, Level.ERROR, message, null);
	}

	public void fatal(Object message) {
		getLogger().log(FQCN, Level.FATAL, message, null);
	}

	public void fatal(Object message, Throwable t) {
		getLogger().log(FQCN, Level.FATAL, message, t);
	}
	
	public void fatal(String format, Object... args) {
		String message = String.format(format, args);
		getLogger().log(FQCN, Level.FATAL, message, null);
	}

	public boolean isDebugEnabled() {
		return getLogger().isDebugEnabled();
	}

	public boolean isErrorEnabled() {
		return getLogger().isEnabledFor(Level.ERROR);
	}

	public boolean isFatalEnabled() {
		return getLogger().isEnabledFor(Level.FATAL);
	}

	public boolean isInfoEnabled() {
		return getLogger().isInfoEnabled();
	}

	public boolean isTraceEnabled() {
		return getLogger().isEnabledFor(traceLevel);
	}

	public boolean isWarnEnabled() {
		return getLogger().isEnabledFor(Level.WARN);
	}
}
