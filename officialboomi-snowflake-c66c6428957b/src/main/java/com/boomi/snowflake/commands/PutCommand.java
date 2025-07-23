// Copyright (c) 2021 Boomi, Inc.
package com.boomi.snowflake.commands;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;

/**
 * The Class PutCommand
 * @author s.vanangudi
 *
 */
public class PutCommand extends InternalStageCommands{
	
	/** The Constant SQL_PUT. */
	private static final String SQL_PUT = "PUT";
	/** The Constant SQL_OVERWRITE. */
	private static final String SQL_OVERWRITE = " OVERWRITE = %s ";
	/** The Constant SQL_AUTO_COMPRESS. */
	private static final String SQL_AUTO_COMPRESS = " AUTO_COMPRESS = %s ";
	/** The Constant SQL_SOURCE_COMPRESSION. */
	private static final String SQL_SOURCE_COMPRESSION = " SOURCE_COMPRESSION = %s ";
	/** The Constant PROP_OVERWRITE. */
	private static final String PROP_OVERWRITE = "overwrite";
	/** The Constant PROP_AUTO_COMPRESS. */
	private static final String PROP_AUTO_COMPRESS = "autoCompress";
	/** The Constant PROP_SOURCE_COMPRESSION. */
	private static final String PROP_SOURCE_COMPRESSION = "sourceCompression";
	/** The Constant DEFAULT_SOURCE_COMPRESSION. */
	private static final String DEFAULT_SOURCE_COMPRESSION = "AUTO_DETECT";
	/** The Constant DEFAULT_AUTO_COMPRESS. */
	private static final boolean DEFAULT_AUTO_COMPRESS = true;
	/** The Constant DEFAULT_OVERWRITE. */
	private static final boolean DEFAULT_OVERWRITE = false;
	/** The Constant PARALLEL_DEFAULT_VALUE. */
	private static final Long PARALLEL_DEFAULT_VALUE = (long) 4;
	/** The Put Command String. */
	private String putCommandSql;
	
	/**
	 * Instantiates a new Put Command
	 * @param operationProperties Put Command Operation Property Map
	 */
	public PutCommand(PropertyMap operationProperties) {
		super();
		putCommandSql = String.format(SQL_COMMAND_FORMAT, 
				SQL_PUT,
				operationProperties.getProperty(PROP_FILE_NAME, ""),
				"@" + operationProperties.getProperty(PROP_INTERNAL_STAGE, ""),
				String.format(SQL_PARALLEL, String.valueOf(operationProperties.getLongProperty(PROP_PARALLEL, PARALLEL_DEFAULT_VALUE))),
				String.format(SQL_AUTO_COMPRESS, operationProperties.getBooleanProperty(PROP_AUTO_COMPRESS, DEFAULT_AUTO_COMPRESS).toString()),
				String.format(SQL_SOURCE_COMPRESSION, operationProperties.getProperty(PROP_SOURCE_COMPRESSION, DEFAULT_SOURCE_COMPRESSION)),
				String.format(SQL_OVERWRITE, operationProperties.getBooleanProperty(PROP_OVERWRITE, DEFAULT_OVERWRITE).toString()));
	}

	/**
	 * Gets the SQL String
	 * @param inputDocument Input Document
	 * @return put Command String
	 */
	@Override
	public String getSQLString(ObjectData inputDocument) {
		return putCommandSql;
	}

}
