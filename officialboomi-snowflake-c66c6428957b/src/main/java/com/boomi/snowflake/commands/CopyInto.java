// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.commands;

import com.boomi.connector.api.PropertyMap;

public abstract class CopyInto implements ISnowflakeCommand {
	
	private static final String PROP_CREDENTIALS = "externalLocationCredentials";
	private static final String PROP_FILE_FORMAT_NAME = "fileFormatName";
	private static final String PROP_FILE_FORMAT_TYPE = "fileFormatType";
	private static final String PROP_FORMAT_COMPRESSION = "formatCompression";
	private static final String PROP_OTHER_FORMAT_OPTIONS = "otherFormatOptions";
	private static final String PROP_COPY_OPTIONS = "copyOptions";
	private static final String SQL_PREDEFINED_FILE_FORMAT = " FILE_FORMAT = ( FORMAT_NAME = %s ) ";
	private static final String SQL_FILE_FORMAT = " FILE_FORMAT = ( TYPE = %s COMPRESSION = %s %s) ";
	protected static final String SQL_COPY_INTO = " COPY INTO %s %s FROM %s %s ";
	
	protected String fileFormat;
	protected String externalLocationCredentials;
	protected String copyOptions;
	
	protected CopyInto(PropertyMap operationProperties) {
		setExternalLocationCredentials(operationProperties);
		setFileFormat(operationProperties);
		setCopyOptions(operationProperties);
	}
	
	private void setExternalLocationCredentials(PropertyMap operationProperties) {
		externalLocationCredentials = operationProperties.getProperty(PROP_CREDENTIALS, "");
	}
	
	private void setFileFormat(PropertyMap operationProperties) {
		if("".equals(operationProperties.getProperty(PROP_FILE_FORMAT_NAME, ""))) {
			fileFormat = String.format(SQL_FILE_FORMAT, 
					operationProperties.getProperty(PROP_FILE_FORMAT_TYPE, ""),
					operationProperties.getProperty(PROP_FORMAT_COMPRESSION, ""),
					operationProperties.getProperty(PROP_OTHER_FORMAT_OPTIONS, ""));
		}else {
			fileFormat = String.format(SQL_PREDEFINED_FILE_FORMAT, operationProperties.getProperty(PROP_FILE_FORMAT_NAME));
		}
	}
	
	private void setCopyOptions(PropertyMap operationProperties) {
		copyOptions = operationProperties.getProperty(PROP_COPY_OPTIONS, "");
	}

	

}
