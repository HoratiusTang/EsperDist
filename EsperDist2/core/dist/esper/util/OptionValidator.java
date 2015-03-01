package dist.esper.util;

import java.io.File;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class OptionValidator {
	public static PrintStream errorStream=System.out;
	public static void setPrintStream(PrintStream newErrorStream){
		errorStream=newErrorStream;
	}
	
	public static boolean checkOptionsExistAtLeastOne(Map<? super String,? super String> context, String[] options){
		for(String option: options){
			if(context.containsKey(option)){
				return true;
			}
		}
		errorStream.println(String.format("error: one of the following options must be specified: %s.", 
				Arrays.toString(options)));

		return false;
	}
	
	public static boolean checkOptionExists(Map<? super String,? super String> context, String option){
		if(!context.containsKey(option)){
			errorStream.println(String.format("error: the option '%s' does not exist.", option));
			return false;
		}
		return true;
	}
	
	public static boolean checkNotNullOrEmpty(Map<? super String,? super String> context, String option){
		if(!checkOptionExists(context, option)){
			return false;
		}
		if(((String)context.get(option)).length()==0){
			errorStream.println(String.format("error: no argument follows option '%s'.", option));
			return false;
		}
		return true;
	}
	
	public static boolean checkNumber(Map<? super String,? super String> context, String option){
		return checkNumberInRange(context, option, Long.MIN_VALUE, Long.MAX_VALUE);
	}
	
	public static boolean checkNumberInRange(Map<? super String,? super String> context, String option, long min, long max){
		if(!checkNotNullOrEmpty(context,option)){
			return false;
		}
		try{
			long number=Long.parseLong((String)context.get(option));
			if(number<min || number>max){
				errorStream.println(String.format("error: the integer '%d' following the option '%s' is out of range [%d, %d].",
						number, option, min, max));
				return false;
			}
		}
		catch(NumberFormatException ex){
			errorStream.println(String.format("error: the argument '%s' is not a valid integer following the option '%s'.",
					context.get(option), option));
			return false;
		}
		return true;
	}
	
	public static boolean checkFileExists(Map<? super String,? super String> context, String option){
		return checkFileOrDirectoryExists(context, option, false);
	}
	
	public static boolean checkDirectoryExists(Map<? super String,? super String> context, String option){
		return checkFileOrDirectoryExists(context, option, true);
	}
	
	public static boolean checkFileOrDirectoryExists(Map<? super String,? super String> context, String option, boolean isDirectory){
		if(!checkNotNullOrEmpty(context,option)){
			return false;
		}
		String filepath=(String)context.get(option);
		File file=new File(filepath);
		if(!file.exists()){
			errorStream.println(String.format("error: the %s '%s' following the option '%s' does not exist.",
					isDirectory?"directory":"file", filepath, option));
			return false;
		}
		if((isDirectory && !file.isDirectory()) || (!isDirectory && !file.isFile())){
			errorStream.println(String.format("error: the path '%s' following the option '%s' is not a valid %s.",
					filepath, option, isDirectory?"directory":"file"));
			return false;
		}
		return true;
	}
	
	public static boolean checkDateFormat(Map<? super String,? super String> context, String option, String dateFormat){
		if(!checkNotNullOrEmpty(context,option)){
			return false;
		}
		String dateStr=(String)context.get(option);
		try {
			SimpleDateFormat sdf=new SimpleDateFormat(dateFormat);
			Date date=sdf.parse(dateStr);
			return true;
		}
		catch (ParseException ex) {
			errorStream.println(String.format("error: the argument '%s' following the option '%s' is not a valid datetime with format '%s'.",
					context.get(option), option, dateFormat));
			return false;
		}
	}
}
