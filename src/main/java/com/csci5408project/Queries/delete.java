package com.csci5408project.Queries;
//Author
//Kandarp Parikh
//B00873863
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.csci5408project.log_management.LogWriterService;
import com.csci5408project.validation.ValidateQuery;

// delete from StudentTable where StudentName = Parth
// delete from StudentTable where StudentID=2002
// delete from table
public class delete {
	Map<String, String> informationMap = new HashMap<>();

	public  void deleteQuery(String query,String databaseName,String userName) throws IOException {
		
		ValidateQuery myQuery = new ValidateQuery();
		long startTime = System.nanoTime();
		if(myQuery.getError(query) == null)
		{
			informationMap.put(LogWriterService.QUERY_LOG_EXECUTED_QUERY_KEY, query);
	        Pattern wherePattern = Pattern.compile("delete from\\s+(.*)\\s+where\\s+(.*)");
	        Matcher matcher = wherePattern.matcher(query);
	        
	        Pattern pattern = Pattern.compile("delete from\\s+(.*)");
	        Matcher matcherWithoutWhere = pattern.matcher(query);
	        
	        matcher.find();
	        matcherWithoutWhere.find();
	        
	        if(query.split(" ").length<4)
	        {
	        	if(checkTableExistence(matcherWithoutWhere.group(1),databaseName) == false)
				{
	        		informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "ERROR : Table does not exist");
					System.out.println("ERROR : Table does not exist");
				}
	        	else
	        	{
	        		writeToFile(deleteAllContents(matcherWithoutWhere.group(1),databaseName),matcherWithoutWhere.group(1),databaseName);
	        	}
	        }
	        else
	        {
				if(checkTableExistence(matcher.group(1),databaseName) == false)
				{
					informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "ERROR : Table does not exist");
					System.out.println("ERROR : Table does not exist");
				}
				else
				{
				writeToFile(deleteRows(query, matcher.group(1),databaseName), matcher.group(1),databaseName);
				}
	        }

	}
		else
		{
			informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, myQuery.getError(query));
			System.out.println(myQuery.getError(query));
		}
	    long stopTime = System.nanoTime();
	    informationMap.put(LogWriterService.GENRAL_LOG_QUERY_EXECUTION_TIME_KEY , ""+(startTime-stopTime));
	    LogWriterService.getInstance().write(informationMap);
		
	}
	
	public  boolean checkTableExistence(String tableName,String databaseName) {
		File tempFile = new File("bin/Databases/"+databaseName+"/"+ tableName+".txt");
		boolean exists = tempFile.exists();
		return exists;
	}
	
    public  Map deleteRows(String query , String tableName,String databaseName) throws IOException
    {
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
		String line;
        Pattern pattern = Pattern.compile("delete from\\s+(.*)\\s+where\\s+(.*)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String whereColumnName = matcher.group(2).split("=")[0].trim();
        String whereColumnValue = matcher.group(2).split("=")[1].trim();
        List<String> tableColumns = new ArrayList<>();
        String[] columnNames = {};
        
		Map tableData = new HashMap();
		int lineNumber = 0;
		int rowsAffected = 0 ;
	    while ((line = br.readLine()) != null) 
	    {
	    	if(line.startsWith("<~row~>") == false)
	    	{
	    		tableData.put(lineNumber, line);
	    		lineNumber += 1;
	    	}
	    	if(line.startsWith("<~colheader~>"))
	    	{
	    		columnNames = line.split("<~colheader~>");
	    		tableColumns = Arrays.asList(columnNames);
	    	}
	    	else if (line.startsWith("<~row~>"))
	    	{
	    		String[] row = line.split("<~row~>");
	    		List<String> rowData = Arrays.asList(row);
	    		if(rowData.get(tableColumns.indexOf(whereColumnName)).equals(whereColumnValue))
	    		{
	    				rowsAffected += 1;
	    		}
	    		else
	    		{
	    			tableData.put(lineNumber, line);
	    			lineNumber += 1;
	    		}
	    	}
	    }
	    System.out.println("Rows affected : "+ rowsAffected);
	    informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CHANGES_KEY , ""+rowsAffected);
	    return tableData;
    }

    public  void writeToFile (Map tableData , String tableName,String databaseName) throws IOException {
    	Writer fileWriter = new FileWriter("bin/Databases/"+databaseName+"/"+tableName+".txt", false);
    	String newLine = System.getProperty("line.separator");
	    for (Object value : tableData.values()) {
	    	fileWriter.write(value.toString() + newLine);
	    }
	    fileWriter.flush();
    }
    
//    public  void deleteAllContents(String tableName,String databaseName) throws FileNotFoundException
//    {
//    	PrintWriter pw = new PrintWriter("bin/Databases/"+databaseName+"/"+tableName+".txt");
//    	pw.close();
//    }
    
    public static Map deleteAllContents(String tableName , String databaseName) throws IOException
    {
		
		String line;
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt" ;
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
    	Map<Integer, String> cleanedTable = new HashMap<>();
    	int linenumber =0;
    	while ((line = br.readLine()) != null) 
	    {
	    	if(line.startsWith("<~row~>") == false)
	    	{
	    		cleanedTable.put(linenumber, line);
	    		linenumber += 1;
	    	}
	    }
    	return cleanedTable;
    }
}
