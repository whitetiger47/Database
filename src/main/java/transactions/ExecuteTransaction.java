package transactions;
//Author
//Kandarp Parikh
//B00873863
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.csci5408project.log_management.LogWriterService;

// insert into StudentTable (StudentName,StudentID,Course) values (Harsh,0001,data)
// insert into StudentTable (StudentName,StudentID,Course) values (Het,0002,Software)
// insert into StudentTable (StudentName,StudentID,Course) values (daks,0003,SDC)
// update StudentTable set StudentName=HetChanged where StudentID = 0002
// update StudentTable set StudentName=HarshChanged where StudentID = 0001
// delete from StudentTable where StudentID=0002

public class ExecuteTransaction {
	Map<String, String> informationMap = new HashMap<>();
public  void beginTransaction(String database) throws IOException {
	
	int commitFlag = 0;
	while(commitFlag == 0)
	{
	System.out.println("Transaction starts");
	Map transactionFile = new HashMap<>();

	String databaseName = database;

	System.out.println("Select TableName : use <Table name>");
	Scanner sc = new Scanner(System.in);
	String s = sc.nextLine();
	String tableName = "";
	if(s.equals("exit"))
	{
		commitFlag = 1;
		break;
	}
	if(s.split(" ").length>1)
	{
		tableName = s.split(" ")[1];
	}

	if(checkLock(tableName)==false)
	{
		String newLine = System.getProperty("line.separator");
		applyLock(tableName);
		informationMap.put(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY, "Lock applied on table : "+tableName);
		long startTime = System.nanoTime();
		transactionFile = getTableData(databaseName,tableName);
		
		// print current state of file
	    for (Object value : transactionFile.values()) {
	    	System.out.println(value.toString() + newLine);
	    }
	    if(queryProcessor(transactionFile,tableName,databaseName)==false)
	    {
	    	//commitFlag = 1;
	    }
	    else
	    {
	    	commitFlag=1;
	    }
	    long stopTime = System.nanoTime();
	    informationMap.put(LogWriterService.GENRAL_LOG_QUERY_EXECUTION_TIME_KEY , ""+(stopTime-startTime));
	}
	else
	{
		informationMap.put(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY, "ERROR : Table is locked , Table name :"+tableName);
		System.out.println("ERROR : Table is locked");
	}
	}
    LogWriterService.getInstance().write(informationMap);
}

public  boolean checkLock(String tableName) throws IOException
{
	String LockManagerLocation = "bin/Databases/LockManager.txt";
	BufferedReader br = new BufferedReader(new FileReader(LockManagerLocation));
    String line;
	    while ((line = br.readLine()) != null) 
	    {
	    	if(line.equals(tableName))
	    	{
	    		return true;
	    	}
	    }
	    return false;
}

public  void applyLock(String tableName)
{
    String newLine = System.getProperty("line.separator");
    String LockManagerLocation = "bin/Databases/LockManager.txt";
	try(PrintWriter output = new PrintWriter(new FileWriter(LockManagerLocation,true))) 
    {
    	output.write(tableName+newLine);
    } 
    catch (Exception e) {}
}

public  void releaseLock(String tableName) throws IOException
{
	Map<Integer, String> locks = new HashMap<>();
	String LockManagerLocation = "bin/Databases/LockManager.txt";
	BufferedReader br = new BufferedReader(new FileReader(LockManagerLocation));
    String line;
    int lineNumber=0;
	    while ((line = br.readLine()) != null) 
	    {
	    	if(line.equals(tableName) == false)
	    	{
	    		locks.put(lineNumber, line);
	    		lineNumber += 1;
	    	}
	    }
    	Writer fileWriter = new FileWriter(LockManagerLocation, false);
    	String newLine = System.getProperty("line.separator");
	    for (Object value : locks.values()) {
	    	fileWriter.write(value.toString() + newLine);
	    }
	    fileWriter.flush();
}

public  Map<Integer, String> getTableData(String databaseName, String tableName) throws IOException
{
	Map<Integer, String> tableData = new HashMap<>();
	String tableLocation = "bin/Databases/"+databaseName+"/"+tableName+".txt";
	BufferedReader br = new BufferedReader(new FileReader(tableLocation));
    String line;
    int lineNumber=0;
    while ((line = br.readLine()) != null) 
    {
		tableData.put(lineNumber, line);
		lineNumber += 1;
    }
    return tableData;
}

public  boolean queryProcessor(Map<Integer, String> transactionFile , String tableName , String databaseName) throws IOException
{
	int commitFlag = 0;
	while(commitFlag == 0)
	{
		System.out.println("Enter query : or enter commit");
		Scanner sc = new Scanner(System.in);
		String query = sc.nextLine();
		String typeOfQuery = query.split(" ")[0];
		
		if(typeOfQuery.equals("insert"))
		{
			insert i = new insert();
			i.insertTransaction(query,transactionFile, databaseName , tableName);
		}
		
		else if(typeOfQuery.equals("update"))
		{
			update u = new update();
			transactionFile = u.updateTransaction(query,transactionFile, databaseName , tableName);
			for(String s : transactionFile.values())
			{
				System.out.println(s);
			}
		}
		
		else if(typeOfQuery.equals("delete"))
		{
			delete u = new delete();
			transactionFile = u.deleteTransaction(query,transactionFile , databaseName , tableName);
		}
		
		else if(typeOfQuery.equals("commit"))
		{
	    	Writer fileWriter = new FileWriter("bin/Databases/"+databaseName+"/"+tableName+".txt", false);
	    	String newLine = System.getProperty("line.separator");
		    for (Object value : transactionFile.values()) {
		    	fileWriter.write(value.toString() + newLine);
		    }
		    fileWriter.flush();
		    releaseLock(tableName);
		    informationMap.put(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY, "Transaction commited to table : "+tableName);
		    informationMap.put(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY, "Lock released on table : "+tableName);
		    break;
	    }
		
		else if(typeOfQuery.equals("rollback"))
		{
		    releaseLock(tableName);
		    informationMap.put(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY, "Rollback Operation performed on table : "+tableName);
		    informationMap.put(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY, "Lock released on table : "+tableName);
		    break;
	    }
		
		}
	return false;
	}
}

