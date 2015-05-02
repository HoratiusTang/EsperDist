package dist.esper.experiment.util;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * a multi-line reader/writer
 * @author tjy
 *
 */
public class MultiLineFileWriter {
	/**
	 * read strings from file. each string can be multi-line style.
	 * blank lines will be treated as separators between strings.
	 * @param filePath the path of file
	 * @return
	 */
	public static List<String> readFromFile(String filePath) throws Exception{
		List<String> strList=new LinkedList<String>();
		BufferedReader br=new BufferedReader(new FileReader(filePath));
		String line=null;
		StringBuilder sb=new StringBuilder();
		while((line=br.readLine())!=null){
			line=line.trim();
			if(line.length()>0 && !line.startsWith("#")){
				sb.append(line);
				sb.append(" \n");
			}
			else{
				if(sb.length()>0){
					strList.add(sb.toString());
					sb.setLength(0);
				}
			}
		}
		if(sb.length()>0){
			strList.add(sb.toString());
			sb.setLength(0);
		}
		br.close();
		return strList;
	}
	
	public static void writeToFile(String filePath, List<String> strList) throws Exception{		
		BufferedWriter bw=new BufferedWriter(new FileWriter(filePath));
		for(String str: strList){
			str=str.trim();
			bw.write(str);
			bw.write('\n');
			bw.write('\n');
		}
		bw.close();
	}
}
