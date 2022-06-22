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

public class Erd {

    public void generateErd() throws IOException {

        Scanner sc = new Scanner(System.in);    //System.in is a standard input stream
        System.out.print("Enter Database Name : ");
        String dbName = sc.nextLine();
        System.out.print("\n");
        File file = new File("bin/Databases/" + dbName);
        try {
            Path path = Paths.get("bin/Erd/" + dbName + "ERD.txt");
            StringBuilder sbFile = new StringBuilder();
            File[] allfiles = file.listFiles();
            if (allfiles != null) {

                for (File individualfile : allfiles) {

                    StringBuilder sbTable = new StringBuilder();

                    String[] colHeaders = {};
                    FileReader fileReader = new FileReader(individualfile);
                    String tableName = individualfile.getName().substring(0, individualfile.getName().length() - 4);

                    sbTable.append("Table Name :").append(tableName).append("\n");

                    BufferedReader br = new BufferedReader(fileReader);
                    String strLine;
                    while ((strLine = br.readLine()) != null) {
                        if (strLine.contains("<~colheader~>")) {
                            colHeaders = strLine.split("<~colheader~>");
                        }
                    }

                    String[] colHeaderDataType = new String[colHeaders.length];
                    for (int i = 0; i < colHeaders.length; i++) {
                        colHeaderDataType[i] = (String.format("%s ", colHeaders[i]));
                    }

                    String headerType = String.join(", ", Arrays.copyOfRange(colHeaderDataType, 1, colHeaderDataType.length));
                    sbTable.append("Column Name:").append(headerType).append("\n");
                    //System.out.println(sbTable);
                    sbFile.append(sbTable).append("\n");
                }

                //System.out.println("---------------------------RelationShip--------------------------");
                sbFile.append("---------------------------RelationShip--------------------------").append("\n");

                for (File individualfile : allfiles) {

                    StringBuilder sbRelation = new StringBuilder();

                    String[] fkeydata = {};
                    List<String> foreignDataList = new ArrayList<>();
                    FileReader fileReader = new FileReader(individualfile);

                    BufferedReader br = new BufferedReader(fileReader);
                    String strLine;
                    while ((strLine = br.readLine()) != null) {
                        if (strLine.contains("foreignkey")) {
                            fkeydata = strLine.split("<~metadata~>");
                            foreignDataList.add(fkeydata[1].split("=")[1]);
                            foreignDataList.add(fkeydata[2].split("=")[1]);
                            foreignDataList.add(fkeydata[3].split("=")[1]);
                        }
                    }
                    if(!foreignDataList.isEmpty()) {
                        String tableName = individualfile.getName().substring(0, individualfile.getName().length() - 4);
                        sbRelation.append(tableName);
                        sbRelation.append(String.format(" (%s) - one to many relationship -  %s ( %s ) \n", foreignDataList.get(0), foreignDataList.get(1), foreignDataList.get(2)));
                        //System.out.println(sbRelation);
                        sbFile.append(sbRelation).append("\n");
                    }
                }

                Files.write(path, sbFile.toString().getBytes());
                System.out.println("ERD Generated");
                System.out.println("Path:" + "bin/Erd/" + dbName +"ERD.txt");
            } else {
                System.out.println("Please Create Database First");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
