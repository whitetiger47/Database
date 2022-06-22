package com.csci5408project.log_management;

import java.util.Map;

public class LogWriterService {
	
	// GENERAL LOGS
	public static final String GENRAL_LOG_QUERY_EXECUTION_TIME_KEY = "QUERY_EXECUTION_TIME";
	public static final String GENRAL_LOG_DATABASE_STATE_KEY = "DATABASE_KEY";
	
	//QUERY LOGS
	public static final String QUERY_LOG_EXECUTED_QUERY_KEY = "EXECUTED_QUERY";
	
	//EVENT LOGS
	public static final String EVENT_LOG_DATABASE_CHANGES_KEY = "DATABASE_CHANGES";
	public static final String EVENT_LOG_TRANSACTIONS_KEY = "TRANSACTIONS";
	public static final String EVENT_LOG_DATABASE_CRASH_KEY = "DATABASE_CRASH";

	private static LogWriterService instance = null;

	public static LogWriterService getInstance() {
		if (instance == null) {
			instance = new LogWriterService();
		}
		return instance;
	}

	LogWriterService() {
		start();
	}
	
	public void start() {
		GeneralLogWriter.getInstance().start();
		EventLogWriter.getInstance().start();
		QueryLogWriter.getInstance().start();
	}

	public void write(Map<String, String> informationMap) {
		if (informationMap.containsKey(GENRAL_LOG_QUERY_EXECUTION_TIME_KEY) && informationMap.containsKey(GENRAL_LOG_DATABASE_STATE_KEY)) {
			GeneralLogWriter.getInstance().writeGeneralLog(informationMap);
		}
		if (informationMap.containsKey(QUERY_LOG_EXECUTED_QUERY_KEY)) {
			QueryLogWriter.getInstance().writeQueryLog(informationMap);
		}
		if (informationMap.containsKey(EVENT_LOG_DATABASE_CHANGES_KEY) || informationMap.containsKey(EVENT_LOG_TRANSACTIONS_KEY)
				|| informationMap.containsKey(EVENT_LOG_DATABASE_CRASH_KEY)) {
			EventLogWriter.getInstance().writeEventLog(informationMap);
		}
		//LogWriterService.getInstance().write(informationMap);
	}
	
	public void stop() {
		GeneralLogWriter.getInstance().stop();
		EventLogWriter.getInstance().stop();
		QueryLogWriter.getInstance().stop();
	}
}
