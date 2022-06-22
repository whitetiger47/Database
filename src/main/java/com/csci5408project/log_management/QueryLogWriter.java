package com.csci5408project.log_management;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Map;

import frontend.Session;

public class QueryLogWriter {

//	private String filePath = "/Users/jaswanth106/Desktop/QueryLogs.txt";
	private String filePath = "bin/Logs/QueryLogs.txt";

	private File file;
	private java.io.FileWriter fileWriter;
	private BufferedWriter bufferedWriter;

	private static QueryLogWriter instance = null;

	public static QueryLogWriter getInstance() {
		if (instance == null) {
			instance = new QueryLogWriter();
		}
		return instance;
	}

	private QueryLogWriter() {
	}

	public void start() {
		try {
			file = new File(this.filePath);
			file.createNewFile();
			this.fileWriter = new java.io.FileWriter(this.file, true);
			this.bufferedWriter = new BufferedWriter(this.fileWriter);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean writeQueryLog(Map<String, String> informationMap) {
		try {
			Calendar calendar = Calendar.getInstance();
			StringBuffer sb = new StringBuffer();
			sb.append("<").append(calendar.getTime()).append(">-");
			sb.append("<").append(Session.getInstance().getLoggedInUser().getUserName()).append(">-");
			// sb.append("<").append("Jaswanth").append(">-");
			sb.append("<").append("Executed Query is : ").append(informationMap.get(LogWriterService.QUERY_LOG_EXECUTED_QUERY_KEY)).append(">");
			this.bufferedWriter.newLine();
			this.bufferedWriter.append(sb.toString());
			this.bufferedWriter.flush();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public void stop() {
		try {
			this.bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
