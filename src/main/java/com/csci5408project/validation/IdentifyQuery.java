package com.csci5408project.validation;

public class IdentifyQuery {
	
	public String identifyQuery(String query) {
		if(query.startsWith("select")) {
			return "SELECT";
		}else if(query.startsWith("insert")) {
			return "INSERT";
		}else if(query.startsWith("update")) {
			return "UPDATE";
		}else if(query.startsWith("delete")) {
			return "DELETE";
		}else if(query.startsWith("delete")) {
			return "DELETE";
		}else if(query.startsWith("create database")) {
			return "CREATE_DATABASE";
		}else if(query.startsWith("create table")) {
			return "CREATE_TABLE";
		}else if(query.startsWith("drop table")) {
			return "DROP_TABLE";
		}else if(query.startsWith("drop database")){
			return "DROP_DATABASE";
		}
		else if(query.startsWith("use database")) {
			return "USE_DATABASE";
		}
		return "";
	}
}

