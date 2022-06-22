package com.csci5408project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class SqlDump {
    public static void generateSqlDump() throws IOException {

        Scanner sc= new Scanner(System.in);    //System.in is a standard input stream
        System.out.print("Enter Database Name : ");
        String dbName= sc.nextLine();
        System.out.print("\n");
        File file = new File("bin/Databases/" + dbName);
        try {
            Path path = Paths.get("bin/Dumpfile/" + dbName +"Dump.txt");
            StringBuilder sbFile = new StringBuilder();
            File[] allfiles = file.listFiles();
            if (allfiles != null) {

                for (File individualfile : allfiles) {

                    StringBuilder sbCreate = new StringBuilder();
                    StringBuilder sbInsert = new StringBuilder();
                    String[] colHeaders = {};
                    String[] colTypes = {};
                    String[] fkeydata = {};
                    String primaryKey = "";
                    String[] row = {};
                    List<String> foreignDataList = new ArrayList<>();
                    List<String[]> rowList = new ArrayList<>();

                    FileReader fileReader = new FileReader(individualfile);
                    String filename = individualfile.getName().substring(0, individualfile.getName().length() - 4);
                    sbCreate.append("CREATE TABLE ").append(filename).append(" ( ");

                    BufferedReader br = new BufferedReader(fileReader);
                    String strLine;
                    while ((strLine = br.readLine()) != null) {
                        if (strLine.contains("<~colheader~>")) {
                            colHeaders = strLine.split("<~colheader~>");
                        } else if (strLine.contains("<~coldatatype~>")) {
                            colTypes = strLine.split("<~coldatatype~>");
                        } else if (strLine.contains("primarykey")) {
                            primaryKey = strLine.split("=")[1];
                        } else if (strLine.contains("foreignkey")) {
                            fkeydata = strLine.split("<~metadata~>");
                            foreignDataList.add(fkeydata[1].split("=")[1]);
                            foreignDataList.add(fkeydata[2].split("=")[1]);
                            foreignDataList.add(fkeydata[3].split("=")[1]);
                        } else if (strLine.contains("<~row~>")) {
                            row = strLine.split("<~row~>");
                            rowList.add(row);
                        }
                    }

                    String[] colHeaderDataType = new String[colHeaders.length];
                    for (int i = 0; i < colHeaders.length; i++) {
                        colHeaderDataType[i] = (String.format("%s %s", colHeaders[i], colTypes[i]));
                    }

                    String headerType = String.join(", ", Arrays.copyOfRange(colHeaderDataType, 1, colHeaderDataType.length));
                    String header = String.join(", ", Arrays.copyOfRange(colHeaders, 1, colHeaders.length));

                    sbCreate.append(headerType);

                    if(!primaryKey.equals("")) {
                        sbCreate.append(String.format(", primary key %s", primaryKey));
                    }

                    if(!foreignDataList.isEmpty()) {
                        sbCreate.append(String.format(", foreign key %s references %s ( %s ) ", foreignDataList.get(0), foreignDataList.get(1), foreignDataList.get(2)));
                    }
                    sbCreate.append(");\n");

                    if(!rowList.isEmpty()) {
                        sbInsert.append("INSERT INTO ").append(filename).append(" ( ");
                        sbInsert.append(header).append(" ) VALUES ");
                        List<String> rowListArray = new ArrayList<>();
                        for (String[] str : rowList) {
                            String strjoin = String.format("(%s)", String.join(", ", Arrays.copyOfRange(str, 1, str.length)));
                            rowListArray.add(strjoin);
                        }
                        sbInsert.append(String.join(", ", rowListArray)).append("\n");
                    }

                    //System.out.println(sbCreate);
                    //System.out.println(sbInsert);

                    sbFile.append(sbCreate).append("\n").append(sbInsert).append("\n");

                }
                Files.write(path, sbFile.toString().getBytes());
                System.out.println("SQL Dump Generated");
                System.out.println("Path:" + "bin/Dumpfile/" + dbName +"Dump.txt");
            }
            else{
                System.out.println("Database does not exist!! Please create database");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
