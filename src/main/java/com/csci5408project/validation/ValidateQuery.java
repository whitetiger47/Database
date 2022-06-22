package com.csci5408project.validation;

public class ValidateQuery {

	// private static final String EXPECTED_SELECT_STATEMENT = "select col1, col2
	// from table_name where condition";
	// private static final String EXPECTED_INSERT_STATEMENT = "insert into
	// table_name (col1,col2) values (val1,val2)";
	// private static final String EXPECTED_UPDATE_STATEMENT = "update table_name
	// set condition";
	// private static final String EXPECTED_DELETE_STATEMENT = "delete from
	// table_name where condition";

	private IdentifyQuery identifyQuery;

	public ValidateQuery() {
		this.identifyQuery = new IdentifyQuery();
	}

	public String getError(String query) {
		query = query.toLowerCase();
		String queryType = this.identifyQuery.identifyQuery(query);
		if (queryType == null) {
			return "Query should start with Create table, create database, Select, Insert, Update, Delete, drop or use database keywords.";
		}
		String error = null;
		String[] wordsInQuery = query.split(" ");
		if (queryType.equalsIgnoreCase("select")) {
			error = getErrorsInSelectStatement(wordsInQuery);
		}
		if (queryType.equalsIgnoreCase("insert")) {
			error = getErrorsInInsertStatement(wordsInQuery);
		}
		if (queryType.equalsIgnoreCase("update")) {
			error = getErrorsInUpdateStatement(wordsInQuery);
		}
		if (queryType.equalsIgnoreCase("delete")) {
			error = getErrorsInDeleteStatement(wordsInQuery);
		}
		return error;
	}

	private String getErrorsInDeleteStatement(String[] wordsInQuery) {
		int deletePosition = 0;
		int fromPosition = 0;
		int wherePosition = 0;
		int i = 1;
		while (i < wordsInQuery.length) {
			if (wordsInQuery[i].equals("from")) {
				fromPosition = i;
			} else if (wordsInQuery[i].equals("where")) {
				wherePosition = i;
			}
			i++;
		}
		if (fromPosition == 0) {
			return "no 'from' keyword found.";
		}
		if (fromPosition - deletePosition != 1) {
			return "'from' keyword should be present after 'delete' keyword.";
		} else if (wherePosition != 0 && wherePosition - fromPosition != 2) {
			return "One Table name should be present between 'from' and 'where' keywords.";
		}
		if (wherePosition == 0 && fromPosition + 1 == wordsInQuery.length) {
			return "table name should be present after 'from' keyword.";
		}
		if (wherePosition != 0 && wherePosition + 1 == wordsInQuery.length) {
			return "'where' keyword used without any conditional statement following the keyword.";
		}
		return null;
	}

	private String getErrorsInUpdateStatement(String[] wordsInQuery) {
		int updatePosition = 0;
		int setPosition = 0;
		int i = 1;
		while (i < wordsInQuery.length) {
			if (wordsInQuery[i].equals("set")) {
				setPosition = i;
			}
			i++;
		}
		if (setPosition == 0) {
			return "no 'set' keyword found.";
		}
		if (setPosition - updatePosition != 2) {
			return "One Table name should be present between 'update' and 'set' keywords.";
		}
		if (setPosition + 1 == wordsInQuery.length) {
			return "'set' keyword used without any values entered after the keyword.";
		}
		return null;
	}

	private String getErrorsInInsertStatement(String[] wordsInQuery) {
		int insertPosition = 0;
		int intoPosition = 0;
		int valuesPosition = 0;
		int i = 1;
		while (i < wordsInQuery.length) {
			if (wordsInQuery[i].equals("into")) {
				intoPosition = i;
			} else if (wordsInQuery[i].equals("values")) {
				valuesPosition = i;
			}
			i++;
		}
		if (intoPosition == 0) {
			return "no 'into' keyword found.";
		}
		if (valuesPosition == 0) {
			return "no 'values' keyword found.";
		}
		if (intoPosition - insertPosition != 1) {
			return "'into' keyword should be present after 'insert' keyword.";
		} else if (valuesPosition - intoPosition <= 2) {
			return "table name and column names should be present between 'into' and 'values' keywords.";
		}
		if (valuesPosition + 1 == wordsInQuery.length) {
			return "'values' keyword used without any values entered after the keyword.";
		}
		return null;
	}

	private String getErrorsInSelectStatement(String[] wordsInQuery) {
		int selectPosition = 0;
		int fromPosition = 0;
		int wherePosition = 0;
		int i = 1;
		while (i < wordsInQuery.length) {
			if (wordsInQuery[i].equals("from")) {
				fromPosition = i;
			} else if (wordsInQuery[i].equals("where")) {
				wherePosition = i;
			}
			i++;
		}
		if (fromPosition == 0) {
			return "no 'from' keyword found.";
		}
		if (fromPosition - selectPosition < 2) {
			return "column names should be present between 'select' and 'from' keywords.";
		} else if (wherePosition - fromPosition < 2 && wherePosition != 0) {
			return "table names should be present between 'from' and 'where' keywords.";
		}
		if (wherePosition == 0 && fromPosition + 1 == wordsInQuery.length) {
			return "table names should be present after 'from' keyword.";
		}
		if (wherePosition != 0 && wherePosition + 1 == wordsInQuery.length) {
			return "'where' keyword used without any conditional statement following the keyword.";
		}
		return null;
	}

//	public static void main(String[] args) {
//		ValidateQuery validate = new ValidateQuery();
//		System.out.println(validate.getError("djhgsjdkhvbhjdbvj"));
//		System.out.println(validate.getError("delete from table_name"));
//	}

}
