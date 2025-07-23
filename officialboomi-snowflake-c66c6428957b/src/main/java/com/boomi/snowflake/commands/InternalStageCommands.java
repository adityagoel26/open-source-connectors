// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.commands;

public abstract class InternalStageCommands implements ISnowflakeCommand{
	
	protected static final String PROP_FILE_NAME = "fileName";
	protected static final String PROP_INTERNAL_STAGE = "internalStage";
	protected static final String SQL_COMMAND_FORMAT = "%s %s %s \n %s\n %s\n %s\n %s";
	protected static final String SQL_PARALLEL = " PARALLEL = %s ";
	protected static final String PROP_PARALLEL = "parallelUpload";
	

	protected InternalStageCommands() {

	}
	
}
