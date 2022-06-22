package com.csci5408project.Queries;

import com.csci5408project.log_management.LogWriterService;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class deleteDatabase {


    Map<String, String> informationMap = new HashMap<>();

    public void deleteDatabaseQuery(String query,String dbName) throws IOException {

        int exitFlag = 0;
        informationMap.put(LogWriterService.QUERY_LOG_EXECUTED_QUERY_KEY, query);

        if(parseDeleteDatabaseQuery(query,dbName) == true)
        {
            exitFlag = 1;
        }
        LogWriterService.getInstance().write(informationMap);

    }

    public static boolean parseDeleteDatabaseQuery(String query, String dbName){

        String deleteDatabase = query.toLowerCase();
        String trimmedQuery = deleteDatabase.trim().replaceAll(" +", " ");

        final Pattern compile = Pattern.compile("drop database (\\w+);");
        final Matcher matcher = compile.matcher(trimmedQuery);
        boolean matchFound = matcher.find();

        if(!matchFound){
            System.out.println("Error occurred query incorrect");
            return false;
        }

        final Pattern getDatabase = Pattern.compile("(?<=\\bdrop database\\s)(\\w+)");
        final Matcher getDatabaseMatcher = getDatabase.matcher(trimmedQuery);
        boolean DBNameBoolean = getDatabaseMatcher.find();

        String databaseName = "";
        if(DBNameBoolean){
            databaseName = getDatabaseMatcher.group();
        }else{
            System.out.println("Error occurred query incorrect");
            return false;
        }


        File file = new File("bin/Databases/");
        String[] databases = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });
        boolean flagFolderExist = false;
        for(int i = 0; i < databases.length; i++){
            if(databases[i].toLowerCase().equals(databaseName)){
                flagFolderExist = true;
                break;
            }
        }
        if(!flagFolderExist){
            System.out.println("Error occurred. Database does not exist.");
            return false;
        }
        File databaseFolder = new File("bin/Databases/" +databaseName);
        try {
            dropDatabase(databaseFolder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static void dropDatabase(File file) throws IOException {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    dropDatabase(entry);
                    System.out.println("Database deleted successfully");
                }
            }
        }
        if (!file.delete()) {
            throw new IOException("Failed to delete " + file);
        }
    }

}
