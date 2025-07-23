// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.commands;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;
import com.boomi.util.StringUtil;

public class GetCommand extends InternalStageCommands{
	
	private final static String OPERATION_NAME = "GET";
	private final static String PROP_PATTERN = "pattern";
	private final static String SQL_PATTERN = " PATTERN = %s ";
	private static final Long PARALLEL_DEFAULT_VALUE = (long) 10;
	private String getCommandSql;
	
	public GetCommand(PropertyMap operationProperties) {
		super();
		getCommandSql = String.format(SQL_COMMAND_FORMAT, OPERATION_NAME, 
				"@" + operationProperties.getProperty(PROP_INTERNAL_STAGE, ""),
				operationProperties.getProperty(PROP_FILE_NAME, ""),
				String.format(SQL_PARALLEL, String.valueOf(operationProperties.getLongProperty(PROP_PARALLEL, PARALLEL_DEFAULT_VALUE))),
				getIfExists(PROP_PATTERN, SQL_PATTERN, operationProperties), "", "");
	}

	private String getIfExists(String propName, String sqlCommand, PropertyMap operationProperties) {
		if(StringUtil.isBlank(operationProperties.getProperty(propName, ""))) {
			return StringUtil.EMPTY_STRING;
		}
		return String.format(sqlCommand, operationProperties.getProperty(propName, ""));
	}
	
	@Override
	public String getSQLString(ObjectData inputDocument) {
		return getCommandSql;
	}

}
