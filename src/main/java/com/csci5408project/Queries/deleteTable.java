package com.csci5408project.Queries;

import Backend.SetDatabase;
import com.csci5408project.log_management.LogWriterService;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class deleteTable {
    private static Map<String, String> informationMap = new HashMap<>();

    public void deleteTableQuery(String query,String dbName) throws IOException {
        int exitFlag = 0;
        informationMap.put(LogWriterService.QUERY_LOG_EXECUTED_QUERY_KEY, query);

        if(parseDeleteTableQuery(query,dbName))
        {
            exitFlag = 1;
        }
        LogWriterService.getInstance().write(informationMap);

    }

    public static boolean parseDeleteTableQuery(String query, String dbName){

        String deleteTable = query.toLowerCase();
        String trimmedQuery = deleteTable.trim().replaceAll(" +", " ");

        final Pattern compile = Pattern.compile("drop table (\\w+);");
        final Matcher matcher = compile.matcher(trimmedQuery);
        boolean matchFound = matcher.find();

        if(!matchFound){
            System.out.println("error occurred Query incorrect");
            return false;
        }

        final Pattern getTable = Pattern.compile("(?<=\\bdrop table\\s)(\\w+)");
        final Matcher getTableMatcher = getTable.matcher(trimmedQuery);
        boolean TableNameBoolean = getTableMatcher.find();

        String tableName = "";
        if(TableNameBoolean){
            tableName = getTableMatcher.group();
        }else{
            System.out.println("Syntax error Line 62");
            return false;
        }


        File folder = new File("bin/Databases/" + dbName);
        File[] listOfFiles = folder.listFiles();
        boolean flagFileExist = false;
        for (File file : listOfFiles) {
            if (file.isFile()) {
                if(file.getName().equals(tableName+".txt")){
                    flagFileExist = true;
                    break;
                }
            }
        }
        if(!flagFileExist){
            System.out.println("Table does not exist. ");
            return false;
        }

        try {
//            File file= new File("bin/Databases/TestDatabase/"+tableName+".txt");
            File file= new File("bin/Databases/"+ dbName + "/" +tableName+".txt");
            if(file.delete()){
                System.out.println("File deleted");
                return true;
            }else {
                System.out.println("could not delete table");
                return false;
            }
        } catch (Exception e){
            System.out.println("Exception occurred ");

        }
        return true;
    }

}
