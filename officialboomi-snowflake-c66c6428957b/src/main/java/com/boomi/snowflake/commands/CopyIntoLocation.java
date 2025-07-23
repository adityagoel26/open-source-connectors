// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.commands;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;

public class CopyIntoLocation extends CopyInto{
	
	private final static String PROP_DESTINATION = "destination";
	private final static String PROP_HEADER = "header";
	private final static String SQL_COMMAND_INCLUDE_HEADER = " HEADER = TRUE ";
	private final static String SQL_COMMAND_EXCLUDE_HEADER = " HEADER = FALSE ";
	
	private String copyIntoLocationSql;

	public CopyIntoLocation(PropertyMap operationProperties, String tableName) {
		super(operationProperties);
		setCopyIntoLocation(operationProperties, tableName);
		
	}
	
	private void setCopyIntoLocation(PropertyMap operationProperties, String tableName) {
		copyIntoLocationSql = String.format(SQL_COPY_INTO, 
				"",
				operationProperties.getProperty(PROP_DESTINATION, ""),
				tableName,
				externalLocationCredentials) + fileFormat + copyOptions + 
				(operationProperties.getBooleanProperty(PROP_HEADER) ? SQL_COMMAND_INCLUDE_HEADER : SQL_COMMAND_EXCLUDE_HEADER);
	}
	
	@Override
	public String getSQLString(ObjectData inputDocument) {
		return copyIntoLocationSql;
	}
}
