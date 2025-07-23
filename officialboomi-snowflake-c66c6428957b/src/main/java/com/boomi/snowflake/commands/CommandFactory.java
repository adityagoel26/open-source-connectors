// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.commands;

import com.boomi.connector.api.PropertyMap;
import com.boomi.snowflake.SnowflakeConnection;

public class CommandFactory {
	private static final String COPY_INTO_TABLE = "copyIntoTable";
	private static final String COPY_INTO_LOCATION = "copyIntoLocation";
	private static final String GET = "GET";
	private static final String PUT = "PUT";
	private static final String SNOWSQL = "snowSQL";

	private CommandFactory() {
		// Prevent initialization
	}
	
	@SuppressWarnings("deprecation")
	public static ISnowflakeCommand getCommand(SnowflakeConnection connection, PropertyMap operationProperties, String tableName) {
		
		if(connection.getOperationContext().getCustomOperationType() == null) {
			return null;
		}
		
		String operationType = connection.getOperationContext().getCustomOperationType();
		switch(operationType) {
			case COPY_INTO_TABLE : return new CopyIntoTable(operationProperties, tableName);
			case COPY_INTO_LOCATION: return new CopyIntoLocation(operationProperties, tableName);
			case GET: return new GetCommand(operationProperties);
			case PUT: return new PutCommand(operationProperties);
			case SNOWSQL: return new SnowSQLCommands(operationProperties);
			default: return null;
		}
	}
}
