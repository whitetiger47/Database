package com.csci5408project.log_management;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Map;

import frontend.Session;

public class EventLogWriter {

//	private String filePath = "/Users/jaswanth106/Desktop/GeneralLogs.txt";
	private String filePath = "bin/Logs/EventsLogs.txt";


	private File file;
	private java.io.FileWriter fileWriter;
	private BufferedWriter bufferedWriter;

	private static EventLogWriter instance = null;

	public static EventLogWriter getInstance() {
		if (instance == null) {
			instance = new EventLogWriter();
		}
		return instance;
	}

	private EventLogWriter() {
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

	public boolean writeEventLog(Map<String, String> informationMap) {
		try {
			Calendar calendar = Calendar.getInstance();
			StringBuffer sb = new StringBuffer();
			sb.append("<").append(calendar.getTime()).append(">-");
			sb.append("<").append(Session.getInstance().getLoggedInUser().getUserName()).append(">-");
			// sb.append("<").append("Jaswanth").append(">-");
			sb.append("<");
			if(informationMap.containsKey(LogWriterService.EVENT_LOG_DATABASE_CHANGES_KEY)) {
				sb.append("The changes made to database are : ").append(informationMap.get(LogWriterService.EVENT_LOG_DATABASE_CHANGES_KEY));
			}
			if(informationMap.containsKey(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY)) {
				sb.append("The running concurrent transactions are : ").append(informationMap.get(LogWriterService.EVENT_LOG_TRANSACTIONS_KEY));
			}
			if(informationMap.containsKey(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY)) {
				sb.append("There has been an database crash due to: ").append(informationMap.get(LogWriterService.EVENT_LOG_DATABASE_CRASH_KEY));
			}
			sb.append(">");
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
