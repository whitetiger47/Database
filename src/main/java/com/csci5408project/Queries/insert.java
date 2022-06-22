package com.csci5408project.Queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
// Author
// Kandarp Parikh
// B00873863
public class insert {
	 Map<String, String> informationMap = new HashMap<>();
	//Check the existence of table
	//Get metadata
	//Get primary key column
	//Get all the primary keys
	//check if the data contains duplicate primary key
	// if no constraints are violated then insert the row in the database
	
	//INSERT INTO table_name (column1, column2, column3, ...)
	//VALUES (value1, value2, value3, ...);
	
	 // insert into StudentTable (StudentName,StudentID,Course) values (Kandarp,9999,data)
	 // insert into StudentTable (StudentName,StudentID,Course) values (Kandarp,B00873863,data)
	 // insert into StudentTable (StudentName,StudentID,Course) values (Kandarp,9999,data)
	 // insert into StudentTable (StudentName,StudentID,address) values (Kandarp,9999,data)
	 // insert into StudentTable (StudentName,Course) values (Kandarp, 8989 ,data)
	 // insert into StudentTable (StudentName,StudentID,StudentID,StudentID) values (Kandarp,9999,data,1234)
	public  void insertQuery(String query, String databaseName , String userName) throws IOException {
		
		long startTime = System.nanoTime();
		ValidateQuery myQuery = new ValidateQuery();
		if(myQuery.getError(query) == null)
			{
			String[] queryArray = query.split(" ");
			String tableName = queryArray[2];
			informationMap.put(LogWriterService.QUERY_LOG_EXECUTED_QUERY_KEY, query);
			if(checkFileExistence(tableName,databaseName) == true && checkColumnExistence(query, databaseName, tableName) == true) 
				{
					if(dataTypeValidation(query, tableName,databaseName) == false)
						{
							informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY,"Error : DataType validation failed");
							System.out.println("Error : DataType validation failed");
						}
					else
					{
						int indexOfPK=getIndexOfPrimaryKey(tableName,databaseName);
						String primaryKey = getPrimaryKey(tableName,databaseName);
						List<String> primaryKeysData = getAllPrimaryKeysData(tableName,indexOfPK,databaseName);
						if(checkDuplicatePrimaryKey(primaryKeysData,query,primaryKey) == true) {
							
						}
						else {
							insertData(tableName,query,databaseName);
						}
					}
				}
			else 
			{

			}
		}
		else {
			informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, myQuery.getError(query));
			System.out.println(myQuery.getError(query));
		}
	    long stopTime = System.nanoTime();
	    informationMap.put(LogWriterService.GENRAL_LOG_QUERY_EXECUTION_TIME_KEY , ""+(startTime-stopTime));
	    LogWriterService.getInstance().write(informationMap);
	}
	
	public  boolean checkFileExistence(String tableName , String databaseName) {
		File tempFile = new File("bin/Databases/"+databaseName+"/"+tableName+".txt");
		boolean exists = tempFile.exists();
		if(exists == false)
		{
			informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "ERROR : Table does not exists , tablename :"+tableName);
			System.out.println("ERROR : Table does not exists");
		}
		
		return exists;
	}
	
	public  String getPrimaryKey(String tableName , String databaseName) throws IOException {
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
		List<String> ColumnList = new ArrayList<>();
	    String line;
	    String primaryKey="";
		    while ((line = br.readLine()) != null) 
		    {
		    	if(line.startsWith("<~metadata~>primarykey"))
		    	{
		    		primaryKey = line.split("=")[1];
		    	}
		    }
		    return primaryKey;
	}
	
	public  Integer getIndexOfPrimaryKey(String tableName , String databaseName) throws IOException
	{
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
		List<String> ColumnList = new ArrayList<>();
	    String line;
	    String primaryKey="";
		    while ((line = br.readLine()) != null) 
		    {
		    	if(line.startsWith("<~metadata~>primarykey"))
		    	{
		    		primaryKey = line.split("=")[1];
		    	}
		    	if(line.startsWith("<~colheader~>"))
		    	{
		    		String[] columnArray = line.split("<~colheader~>");
		    		ColumnList = Arrays.asList(columnArray);
		    	}
		    }
		return ColumnList.indexOf(primaryKey);
	}

	public  List<String> getAllPrimaryKeysData(String tableName,int indexOfPK ,String databaseName) throws IOException
	{
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
		List<String> primaryKeyData = new ArrayList<>();
		String line;
	    while ((line = br.readLine()) != null) 
	    {
	    	if(line.startsWith("<~row~>"))
	    	{
	    		primaryKeyData.add(line.split("<~row~>")[indexOfPK]);
	    	}
	    }
	    return primaryKeyData;
	}
	
	public  boolean checkDuplicatePrimaryKey(List<String> primaryKeysData , String query , String primaryKey)
	{
        Pattern pattern = Pattern.compile("insert into\\s+(.*?)\\s+\\((.*?)\\)\\s+values\\s+\\((.*?)\\)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String tableName = matcher.group(1);
        String[] columnName = matcher.group(2)
                .replaceAll("\\s+", "")
                .split(",");
        List<String> collist = Arrays.asList(columnName);
        
        String[] values = matcher.group(3)
               // .replaceAll("\\s+", "")
                .split(",");
        int primaryKeyIndex = collist.indexOf(primaryKey);
        if(primaryKeyIndex == -1)
        {
        	informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "ERROR : Primary Key Column cant be empty , Primary Key :"+primaryKey);
        	System.out.println("ERROR : Primary Key Column cant be empty");
        	return true;
        }
        if(columnName.length != values.length)
        {
        	informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "ERROR : Number of column names specified should match the number of values in the query");    	
        	System.out.println("ERROR : Number of column names specified should match the number of values in the query");
        	return true;
        }
        List<String> insertValues = Arrays.asList(values);
        if(primaryKeysData.contains(insertValues.get(primaryKeyIndex)))
        {
        	informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "ERROR : Duplicate Primary Key");    	
        	System.out.println("ERROR : Duplicate Primary Key");
        	return true;
        }
        else {
        	return false;
        }
        
	}
	
	public  void insertData(String tableName, String query ,String databaseName) throws IOException
	{
        Pattern pattern = Pattern.compile("insert into\\s+(.*?)\\s+\\((.*?)\\)\\s+values\\s+\\((.*?)\\)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();
     
        String[] columnName = matcher.group(2)
                .replaceAll("\\s+", "")
                .split(",");
        List<String> collist = Arrays.asList(columnName);
        
        String[] values = matcher.group(3)
                //.replaceAll("\\s+", "")
                .split(",");
        
        Map<String, String> columnRowsMap = new HashMap();
        for(int i = 0 ; i<collist.size() ; i++)
        {
        	columnRowsMap.put(columnName[i],values[i]);
        }
        
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
		List<String> ColumnList = new ArrayList<>();
	    String line;
	    while ((line = br.readLine()) != null) 
	    {
	    	if(line.startsWith("<~colheader~>"))
	    	{
	    		String[] columnArray = line.split("<~colheader~>");
	    		ColumnList = Arrays.asList(columnArray);
	    	}
	    }
	    
	    String insertString = "";
	    String newLine = System.getProperty("line.separator");
	    for(int i=1;i<ColumnList.size();i++)
	    {
	    	insertString = insertString + "<~row~>"+columnRowsMap.get(ColumnList.get(i));	
	    }
	    informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CHANGES_KEY, "Added row"+insertString);
	    try(PrintWriter output = new PrintWriter(new FileWriter(tableLocation,true))) 
	    {
	    	output.write(newLine + insertString);
	    } 
	    catch (Exception e) {}
	}
	
	public  boolean dataTypeValidation(String query , String tableName ,String databaseName) throws IOException
	{
		int validationFlag = 0;
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
		List<String> dataTypeList = new ArrayList<>();
		List<String> tableColumns = new ArrayList<>();
	    String line;
        Pattern pattern = Pattern.compile("insert into\\s+(.*?)\\s+\\((.*?)\\)\\s+values\\s+\\((.*?)\\)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();

        String[] queryColumnName = matcher.group(2).replaceAll("\\s+", "").split(",");
        List<String> querycolumnlist = Arrays.asList(queryColumnName);
        
        String[] values = matcher.group(3).split(",");
	    while ((line = br.readLine()) != null) 
	    {
	    	if(line.startsWith("<~coldatatype~>"))
	    	{
	    		String[] dataTypes = line.split("<~coldatatype~>");
	    		dataTypeList = Arrays.asList(dataTypes);
	    	}
	    	
	    	if(line.startsWith("<~colheader~>"))
	    	{
	    		String[] columnHeader = line.split("<~colheader~>");
	    		tableColumns = Arrays.asList(columnHeader);
	    	}
	    }
	    for(int i=0;i<querycolumnlist.size();i++)
	    {
	    	String temp = dataTypeList.get(tableColumns.indexOf(querycolumnlist.get(i)));
//	    	System.out.println(querycolumnlist.get(i) +" : "+temp);
	    	if(temp.equals("int") && (values[i].matches("\\d+") == false))
	    	{
	    		validationFlag = 1;
	    	}
	    }
	    if(validationFlag == 1)
	    {
	    	return false;
	    }
	    else
	    {
	    	return true;
	    }
	}
	
	public boolean checkColumnExistence(String query , String databaseName , String tableName) throws IOException
	{
        Pattern pattern = Pattern.compile("insert into\\s+(.*?)\\s+\\((.*?)\\)\\s+values\\s+\\((.*?)\\)");
        Matcher matcher = pattern.matcher(query);
        matcher.find();
     
        String[] columnName = matcher.group(2)
                .replaceAll("\\s+", "")
                .split(",");
        List<String> collist = Arrays.asList(columnName);
        
		String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
		BufferedReader br = new BufferedReader(new FileReader(tableLocation));
		List<String> ColumnList = new ArrayList<>();
	    String line;
	    while ((line = br.readLine()) != null) 
	    {
	    	if(line.startsWith("<~colheader~>"))
	    	{
	    		String[] columnArray = line.split("<~colheader~>");
	    		ColumnList = Arrays.asList(columnArray);
	    	}
	    }
	    
    	if(ColumnList.size() > collist.size())
    	{
    	    for(int i=0; i<collist.size();i++)
    	    {
		    	if(ColumnList.contains(collist.get(i)) == false)
				{
		    		informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "entered column value :"+collist.get(i)+" does not exist in table "+ tableName);
		    		System.out.println("entered column value :"+collist.get(i)+" does not exist in table "+ tableName);
	    			return false;
				}
    	    }
    	    return true;
    	}
    	else
    	{
    		informationMap.put(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY, "Entered number of columns are greater than number of columns in table");
    		System.out.println("Entered number of columns are greater than number of columns in table");
    		return false;
    	}      
	}
}
