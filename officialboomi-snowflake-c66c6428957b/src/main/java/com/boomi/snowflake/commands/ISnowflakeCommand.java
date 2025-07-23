// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.commands;

import com.boomi.connector.api.ObjectData;

public interface ISnowflakeCommand {
	
	/**
	 * @param inputDocument input document data
	 * 
	 * @return SQL command String to be executed on Snowflake
	 */
	public String getSQLString(ObjectData inputDocument);
	
}
