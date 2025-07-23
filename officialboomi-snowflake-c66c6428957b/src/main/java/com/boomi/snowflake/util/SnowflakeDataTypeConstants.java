// Copyright (c) 2021 Boomi, Inc.
package com.boomi.snowflake.util;

/**
 * The Class SnowflakeDataTypeConstants
 * @author s.vanangudi
 *
 */
public class SnowflakeDataTypeConstants {

	
	private SnowflakeDataTypeConstants() {
		super();
	}
	/** The Constant SNOWFLAKE_CHARACTER. */
	public static final String SNOWFLAKE_CHARACTER = "string";
	/** The Constant SNOWFLAKE_BOOLEAN. */
	public static final String SNOWFLAKE_BOOLEAN = "boolean";
	/** The Constant SNOWFLAKE_DATE. */
	public static final String SNOWFLAKE_DATE = "date";
	/** The Constant SNOWFLAKE_TIME. */
	public static final String SNOWFLAKE_TIME = "time";
	/** The Constant SNOWFLAKE_DATETIME. */
	public static final String SNOWFLAKE_DATETIME = "date-time";
	/** The Constant SNOWFLAKE_NUMBER. */
	public static final String SNOWFLAKE_NUMBER = "number";
	/** The Constant SNOWFLAKE_NUMBER. */
	public static final String SNOWFLAKE_NUMBERTYPE = "NUMBER";
	/** The Constant SNOWFLAKE_FLOAT. */
	public static final String SNOWFLAKE_FLOAT = "DOUBLE";
	/** The Constant SNOWFLAKE_FLOATTYPE. */
	public static final String SNOWFLAKE_FLOATTYPE = "FLOAT";
	/** The Constant SNOWFLAKE_DATETIMETYPE. */
	public static final String SNOWFLAKE_DATETYPE = "DATE";
	/** The Constant SNOWFLAKE_DATETIMETYPE. */
	public static final String SNOWFLAKE_TIMETYPE = "TIME";
	/** The Constant SNOWFLAKE_DATETIMETYPE. */
	public static final String SNOWFLAKE_DATETIMETYPE = "DATETIME";
	/** The Constant SNOWFLAKE_TIMESTAMP_NTZ. */
	public static final String SNOWFLAKE_TIMESTAMP_NTZ = "TIMESTAMPNTZ";
	/** The Constant SNOWFLAKE_TIMESTAMP_LTZ. */
	public static final String SNOWFLAKE_TIMESTAMP_LTZ = "TIMESTAMPLTZ";
	/** The Constant SNOWFLAKE_TIMESTAMP_TZ. */
	public static final String SNOWFLAKE_TIMESTAMP_TZ = "TIMESTAMPTZ";
	/** The Constant SNOWFLAKE_BOOLEAN. */
	public static final String SNOWFLAKE_BOOLEANTYPE = "BOOLEAN";
	/** The Constant SNOWFLAKE_VARCHARTYPE. */
	public static final String SNOWFLAKE_VARCHARTYPE = "VARCHAR";
	/** The Constant SNOWFLAKE_TIMESTAMPTYPE. */
	public static final String SNOWFLAKE_TIMESTAMPTYPE = "TIMESTAMP";
	/** The Constant SNOWFLAKE_TIMESTAMPTYPE. */
	public static final String SNOWFLAKE_BINARYTYPE = "BINARY";
	/** The Constant SNOWFLAKE_DOUBLETYPE. */
	public static final String SNOWFLAKE_DOUBLETYPE = "DOUBLE";
	/** The Constant SNOWFLAKE_TRUE. */
	public static final String SNOWFLAKE_TRUE = "true";
	/** The Constant SNOWFLAKE_FALSE. */
	public static final String SNOWFLAKE_FALSE = "false";
	/** The default Date Time Format. */
	public static final String DATETIMEFORMAT = "yyyyMMdd HHmmss.SSS";
	/** The Constant Time Format. */
	public static final String TIMEFORMAT = "HHmmss.SSS";
	/** The Constant Date Format. */
	public static final String DATEFORMAT = "MMddyyyy";
	/** The Constant DATE_OUTPUT_FORMAT. */
	public static final String DATE_INPUT_FORMAT = "DATE_INPUT_FORMAT";
	/** The Constant TIME_OUTPUT_FORMAT. */
	public static final String TIME_INPUT_FORMAT = "TIME_INPUT_FORMAT";
	/** The Constant TIMESTAMP_OUTPUT_FORMAT. */
	public static final String TIMESTAMP_INPUT_FORMAT = "TIMESTAMP_INPUT_FORMAT";
	/** The Constant NULL_SELECTION. */
	public static final String NULL_SELECTION = "SELECT_NULL";
	/** The Constant DEFAULT_SELECTION. */
	public static final String DEFAULT_SELECTION = "SELECT_DEFAULT";
	/** The Constant BUILDING_SQL_ERROR. */
	public static final String BUILDING_SQL_ERROR = "Errors occurred while building SQL statement";
	/** The Constant MISSING_VALUE_ERROR. */
	public static final String MISSING_COLUMN_VALUE_ERROR = "Missing column value: ";
	/** The Constant EMPTY_JSON_ERROR. */
	public static final String EMPTY_JSON_ERROR = "Error occurred as user is trying to send empty JSON file input";
	/** The Constant SQL_EXECUTION_ERROR. */
	public static final String SQL_EXECUTION_ERROR ="Errors occurred while executing SQL statement";
	/** The Constant FETCH_DATA_ERROR. */
	public static final String FETCH_DATA_ERROR ="Unable to fetch data";
	
	
}
