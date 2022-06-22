package com.csci5408project.Queries;

import com.csci5408project.log_management.LogWriterService;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class createTable {

    private static Map<String, String> informationMap = new HashMap<>();

    public void createTableQuery(String query, String dbName) throws IOException {

        int exitFlag = 0;
        informationMap.put(LogWriterService.QUERY_LOG_EXECUTED_QUERY_KEY, query);

        if(parseTableQuery(query,dbName))
        {
            exitFlag = 1;
        }
        LogWriterService.getInstance().write(informationMap);

    }

    public boolean parseTableQuery(String query,String dbName) throws IOException {


//        query = "create table student_table (id int,name string,column3 string,PRIMARY KEY id);".toLowerCase();
//        query = "create table student_details(student_id int,subjects int,primary key student_id,foreign key student_id references student_table(id));";
//        query = "create table employee(id int,salary int,name string,primary key id,foreign key id references department(id));";
//        query = "create table managers(id int,goals int,department_id int,foreign key department_id references department(id));";
//        query = "create table department(id int,name string,primary key id);";
        String createTable = query.toLowerCase();

        String trimmedQuery = createTable.trim().replaceAll(" +", " ");

        final Pattern compile = Pattern.compile("create table \\w+( *)\\((\\w+\\s(string|int),( *))*(\\w+\\s(string|int))((,)( *)primary key \\w+|)((,)foreign key \\w+ references \\w+\\(\\w+\\)|)\\);");

        final Matcher matcher = compile.matcher(trimmedQuery);
        boolean matchFound = matcher.find();

        if(!matchFound){
            System.out.println("Error Occurred Query incorrect");
            return false;
        }

        final Pattern getTable = Pattern.compile("(?<=\\bcreate table\\s)(\\w+)");
        final Matcher getTableMatcher = getTable.matcher(trimmedQuery);
        boolean tableNameBoolean = getTableMatcher.find();
        String tableName = getTableMatcher.group();
        String getColumnsString = trimmedQuery.trim().replaceAll(",( *)", ",");
        final Pattern compileColumns = Pattern.compile("(\\w+\\s(string|int),)*(( *)\\w+\\s(string|int))");
        final Matcher getColumns = compileColumns.matcher(getColumnsString);
        boolean matchFound2 = getColumns.find();
        if(!matchFound2){
             System.out.println("An error occurred");
            return false;
        }

        String columns = getColumns.group();

        String[] columnStrings = columns.split(",");
        List<String> columnNames = new ArrayList<>();
        List<String> columnDataTypes = new ArrayList<>();

        for(int i = 0; i < columnStrings.length;i++){
            String temp = columnStrings[i].trim();
            String[] columnValues = temp.split(" ");
            columnNames.add(columnValues[0]);
            columnDataTypes.add(columnValues[1]);
        }

        boolean flagPrimaryKey = false;
        String parsedPrimaryKey = "";

        //  to find primary key in string
        final Pattern primaryKeyPattern = Pattern.compile("(?<=\\bprimary key\\s)(\\w+)");
        final Matcher getPrimaryKey = primaryKeyPattern.matcher(trimmedQuery);
        boolean primaryKeyExist = getPrimaryKey.find();

        String primaryKey = "";

        File folder = new File("bin/Databases/"+ dbName);
        File[] listOfFiles = folder.listFiles();

        boolean flagCheckTable = checkTableExist(tableName,folder);
        if(flagCheckTable){
            System.out.println("Table Name exists");
            return false;
        }
        boolean flagFileExist = false;
        boolean referenceTableExist = false;

        final Pattern foreignKeyPattern = Pattern.compile("(?<=\\bforeign key\\s)(\\w+)");
        final Matcher getForeignKey = foreignKeyPattern.matcher(trimmedQuery);
        boolean foreignKeyExist = getForeignKey.find();

        String referenceTableName = "";
        String referenceColumn = "";
        if(foreignKeyExist){
            String foreignKey = "";
            if(foreignKeyExist){
                foreignKey = getForeignKey.group();
            }

//        (?<=\breferences\s)(\w+\(\w+\))
            final Pattern referencesPattern = Pattern.compile("(?<=\\breferences\\s)(\\w+\\(\\w+\\))");
            final Matcher getReferences = referencesPattern.matcher(trimmedQuery);
            boolean referenceExist = getReferences.find();
            String referencesString = "";
            if(referenceExist){
                referencesString = getReferences.group();
            }

            String[] references = referencesString.split("\\(");
            if(foreignKeyExist) {
                for (int i = 0; i < references.length; i++) {
                    referenceTableName = references[0].trim();
                    referenceColumn = references[1].trim();
                    referenceColumn = referenceColumn.replaceAll("\\)", "");
                }
            }
        }

        for (File file : listOfFiles) {
            if (file.isFile()) {

                if(file.getName().equals(tableName+".txt")){
                    flagFileExist = true;
                    break;
                }

                if(file.getName().equals(referenceTableName+".txt")){
                    referenceTableExist = true;
                }
            }
        }

        if(flagFileExist){
            System.out.println("Table already exist");
            return false;
        }

        if(foreignKeyExist) {
            if (!referenceTableExist) {
                System.out.println("Referenced table does not exist");
                return false;
            }
        }
        if(foreignKeyExist) {
            boolean checkForeignKey = checkForeignKeyExist(referenceTableName, referenceColumn,dbName);
            if(!checkForeignKey){
                System.out.println("Foreign key reference incorrect");
                return false;
            }
        }

        String foreignKey = "";
        if(foreignKeyExist){
            foreignKey = getForeignKey.group();
        }

        if(primaryKeyExist){
            primaryKey = getPrimaryKey.group();
        } else{
            if(foreignKeyExist){
                writeToFile(tableName,false,"",columnNames,columnDataTypes,true,referenceColumn,referenceTableName,foreignKey,dbName);
            }else{
                writeToFile(tableName,false,"",columnNames,columnDataTypes,false,"","","",dbName);
            }
            return true;
        }
        parsedPrimaryKey = primaryKey;
        boolean flagForeignKey = false;
        for (int i = 0; i < columnNames.size(); i++) {
            if(parsedPrimaryKey.equals(columnNames.get(i))){
                flagPrimaryKey = true;
            }
        }
        for (int i = 0; i < columnNames.size(); i++) {
            if(foreignKey.equals(columnNames.get(i))){
                flagForeignKey = true;
            }
        }
        if(!flagPrimaryKey){
            System.out.println("ERROR OCCURRED PLEASE ENTER CORRECT PRIMARY KEY");
            return false;
        }
        if(foreignKeyExist) {
            if (!flagForeignKey) {
                System.out.println("ERROR OCCURRED PLEASE ENTER CORRECT FOREIGN KEY");
                return false;
            }
        }

        writeToFile(tableName,primaryKeyExist,parsedPrimaryKey,columnNames,columnDataTypes,flagForeignKey,referenceColumn,referenceTableName,foreignKey,dbName);
        return true;

    }

    public static void writeToFile(String tableName, boolean primaryKeyExist, String primaryKey, List columnNames, List columnDataTypes,boolean foreignKeyExist,String foreignKey,String referenceTableName, String currentTableColumn,String dbName) {
        LogWriterService logWriterService = LogWriterService.getInstance();
        String colHeaders = "";
        String colHeadersDatatype = "";
        for (int i = 0; i < columnNames.size(); i++) {
            colHeaders+= "<~colheader~>" + columnNames.get(i);
            colHeadersDatatype += "<~coldatatype~>" + columnDataTypes.get(i);
        }
        String table = "<~tablename~>" + tableName;

        try {
            PrintWriter writer = new PrintWriter("bin/Databases/"+dbName +"/" + tableName+".txt", "UTF-8");
            writer.println(table);

            if(primaryKeyExist){
                String metaDataPK = "<~metadata~>primarykey=" + primaryKey;
                writer.println(metaDataPK);
            }
            if(foreignKeyExist){
                String metaDataFK = "<~metadata~>foreignkey=" + currentTableColumn +"<~metadata~>tablename=" + referenceTableName+"<~metadata~>tablecolumnname="+foreignKey;
//                String metaDataFKTableName = "<~metadata~>foreignkeyTableName=" + referenceTableName;
                writer.println(metaDataFK);
//                writer.println(metaDataFKTableName);
            }
            System.out.println("Table successfully created");
            writer.println(colHeaders);
            writer.println(colHeadersDatatype);
            writer.close();

        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }


    public boolean checkForeignKeyExist(String tableName,String foreignKeyColumn,String dbName) throws IOException
    {
        String tableLocation = "bin/Databases/"+dbName + "/" +tableName+".txt";
        BufferedReader br = new BufferedReader(new FileReader(tableLocation));
        List<String> ColumnList = new ArrayList<>();
        String line;
        String primaryKey="";
        while ((line = br.readLine()) != null)
        {
            if(line.startsWith("<~colheader~>"))
            {
                String[] columnArray = line.split("<~colheader~>");
                System.out.println("");
                ColumnList = Arrays.asList(columnArray);
            }
        }

        br.close();
        if(ColumnList.contains(foreignKeyColumn)){
            return true;
        }else{
            return false;
        }


    }


    public boolean checkTableExist(String tableName,File filePath) throws IOException{
        File[] listOfFiles = filePath.listFiles();
        boolean flagFileExist = false;
        boolean referenceTableExist = false;
        for (File file : listOfFiles) {
            if (file.isFile()) {

                if(file.getName().equals(tableName+".txt")){
                    flagFileExist = true;
                    return true;
                }

//                if(file.getName().equals(referenceTableName+".txt")){
//                    referenceTableExist = true;
//                }
            }
        }
        return false;
    }


    public void foreignKeyInformation(String query,File filePath){
//        Line 98 to 130
        final Pattern foreignKeyPattern = Pattern.compile("(?<=\\bforeign key\\s)(\\w+)");
        final Matcher getForeignKey = foreignKeyPattern.matcher(query);
        boolean foreignKeyExist = getForeignKey.find();

        String foreignKey = "";
        if(foreignKeyExist){
            foreignKey = getForeignKey.group();
        }
        System.out.println("foreignKey: "+foreignKey);

//        (?<=\breferences\s)(\w+\(\w+\))
        final Pattern referencesPattern = Pattern.compile("(?<=\\breferences\\s)(\\w+\\(\\w+\\))");
        final Matcher getReferences = referencesPattern.matcher(query);
        boolean referenceExist = getReferences.find();
        String referencesString = "";
        if(referenceExist){
            referencesString = getReferences.group();
        }
        System.out.println("referencesString: "+referencesString);

        String[] references = referencesString.split("\\(");
        String referenceColumn = "";
        String referenceTableName = "";
        if(foreignKeyExist) {
            for (int i = 0; i < references.length; i++) {
                referenceTableName = references[0].trim();
                referenceColumn = references[1].trim();
                referenceColumn = referenceColumn.replaceAll("\\)", "");
//                System.out.println("Hello World: " + columnStrings[i]);
            }
        }
        System.out.println("referenceTableName: "+referenceTableName);
        System.out.println("referenceColumn: "+referenceColumn);
    }
}
