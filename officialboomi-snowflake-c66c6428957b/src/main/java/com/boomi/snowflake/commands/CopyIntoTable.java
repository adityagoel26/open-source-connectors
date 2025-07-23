// Copyright (c) 2024 Boomi, LP.

package com.boomi.snowflake.commands;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;
import com.boomi.util.StringUtil;

public class CopyIntoTable extends CopyInto{

	private static final String PROP_SOURCE = "source";
	private static final String PROP_FILES = "files";
	private static final String PROP_PATTERN = "pattern";
	private static final String PROP_COLUMNS = "columns";
	private static final String SQL_COMMAND_FILES = " FILES = ";
	private static final String SQL_COMMAND_PATTERN = " PATTERN =  ";
	public static final char RIGHT_PARENTHESIS = ')';
	public static final char LEFT_PARENTHESIS = '(';

	private String copyIntoTableSql;
	
	public CopyIntoTable(PropertyMap operationProperties, String tableName) {
		super(operationProperties);
		setCopyIntoTable(operationProperties, tableName);
	}
	
	private static String getColNames(PropertyMap operationProperties) {
		String colNames = operationProperties.getProperty(PROP_COLUMNS, "");
		return !StringUtil.isEmpty(colNames) ?
				String.format("%c%s%c",LEFT_PARENTHESIS,colNames,RIGHT_PARENTHESIS) :
				StringUtil.EMPTY_STRING;
	}
		
	private static String getFileNamesOrPattern(PropertyMap operationProperties, String prop, String sql) {
		String files = operationProperties.getProperty(prop, "");
		return !StringUtil.isEmpty(files) ? (sql + files):StringUtil.EMPTY_STRING ;
	}
	
	private void setCopyIntoTable(PropertyMap operationProperties, String tableName) {
		copyIntoTableSql = String.format(SQL_COPY_INTO,
				tableName,
				getColNames(operationProperties),
				operationProperties.getProperty(PROP_SOURCE, ""),
				externalLocationCredentials) +
				getFileNamesOrPattern(operationProperties, PROP_FILES, SQL_COMMAND_FILES) +
				getFileNamesOrPattern(operationProperties, PROP_PATTERN, SQL_COMMAND_PATTERN) +
				fileFormat + copyOptions;
	}
	
	@Override
	public String getSQLString(ObjectData inputDocument) {
		return copyIntoTableSql;
	}

}
