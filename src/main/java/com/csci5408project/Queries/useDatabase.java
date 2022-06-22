package com.csci5408project.Queries;

import Backend.SetDatabase;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class useDatabase {

    public static void main(String[] args) throws IOException {
        useDatabaseQuery();
    }

    public static void useDatabaseQuery() throws IOException {
        Scanner sc = new Scanner(System.in);
        int exitFlag = 0;
        while (exitFlag == 0) {
            System.out.println("Enter query");
            String query = sc.nextLine();
            if (parseUseDatabaseQuery(query) == true) {
                exitFlag = 1;
            }
        }
    }

    public static boolean parseUseDatabaseQuery(String query){

        query = query.toLowerCase();
        String useDBQuery = query.trim().replaceAll(" +", " ");
        final Pattern useDBCompile = Pattern.compile("use (\\w+);");

        System.out.println("useDBQuery : " +useDBQuery);;
        final Matcher useMatcher = useDBCompile.matcher(useDBQuery);
        boolean databaseMatchFound = useMatcher.find();

        if(!databaseMatchFound){
            System.out.println("ERROR OCCURRED Query incorrect LINE 43");
            return false;
        }

        final Pattern getDatabase = Pattern.compile("(?<=\\buse\\s)(\\w+)");
        final Matcher getDatabaseMatcher = getDatabase.matcher(useDBQuery);
        boolean DatabaseNameBoolean = getDatabaseMatcher.find();

        System.out.println("DatabaseNameBoolean: " + DatabaseNameBoolean);

        String databaseName = "";
        if(DatabaseNameBoolean){
            databaseName = getDatabaseMatcher.group();
        }else{
            System.out.println("Syntax error");
            return false;
        }

        File file = new File("bin/Databases");
        boolean dbMatchFound = false;
        String[] databases = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });

        for(int i = 0; i < databases.length; i++){

            if(databases[i].toLowerCase().equals(databaseName)){
                System.out.println("Error occurred Database exist");
                dbMatchFound =  true;
                SetDatabase.getInstance().setDb(databaseName);
                break;
            }
        }

        if(!dbMatchFound){
            System.out.println("Database does not exist");
            return false;
        }
        SetDatabase.getInstance().setDb(databaseName);

        return true;
    }



}
