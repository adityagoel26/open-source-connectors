// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.constants;

/**
 * The Class DatabaseConnectorConstants.
 *
 * @author swastik.vn
 */
public class DatabaseConnectorConstants {

	public static final String CUSTOM_PROPERTIES = "CustomProperties";

	public static final String CONNECT_TIME_OUT = "connectTimeOut";

	public static final String READ_TIME_OUT = "readTimeOut";

	/** The Constant INTEGER. */
	public static final String INTEGER = "integer";

	/** The Constant STRING. */
	public static final String STRING = "string";
	
	/** The Constant DOUBLE. */
	public static final String DOUBLE = "double";
	
	/** The Constant FLOAT. */
	public static final String FLOAT = "float";
	
	/** The Constant TIMESTAMP. */
	public static final String TIMESTAMP = "timestamp";

	/** The Constant DATE. */
	public static final String DATE = "date";
	
	/** The Constant DATE. */
	public static final String NVARCHAR = "nvarchar";

	/** The Constant TIME. */
	public static final String TIME = "time";

	/** The Constant LONG. */
	public static final String LONG = "long";
	
	/** The Constant JSON. */
	public static final String JSON = "JSON";
	
	/** The Constant BLOB. */
	public static final String BLOB = "BLOB";
	
	/** The Constant CLOB. */
	public static final String CLOB = "CLOB";
	
	/** The Constant IN. */
	public static final String IN = "IN";
	
	/** The Constant BINARY_DOUBLE. */
	public static final String BINARY_DOUBLE = "BINARY_DOUBLE";

	/** The Constant BOOLEAN. */
	public static final String BOOLEAN = "boolean";

	/** The Constant DUPLICATE_PRIMARY_KEY. */
	public static final String DUPLICATE_PRIMARY_KEY = "1062";
	
	/** The Constant LAST_ACCESS_TIME. */
	public static final Long LAST_ACCESS_TIME = 21600000L;
	
	/** The Constant MAX_IDLE. */
	public static final int MAX_IDLE = 50;

	/** The Constant QUERY_INITIAL. */
	public static final String QUERY_INITIAL = "Insert into ";

	/** The Constant QUERY_VALUES. */
	public static final String QUERY_VALUES = ") values (";

	/** The Constant SUCCESS_RESPONSE_CODE. */
	public static final String SUCCESS_RESPONSE_CODE = "200";

	/** The Constant SUCCESS_RESPONSE_MESSAGE. */
	public static final String SUCCESS_RESPONSE_MESSAGE = "Ok";

	/** The Constant QUERY. */
	public static final String QUERY = "query";

	/** The Constant SELECT_INITIAL. */
	public static final String SELECT_INITIAL = "Select * from ";

	/** The Constant JSON_DRAFT4_DEFINITION. */
	public static final String JSON_DRAFT4_DEFINITION = "\n\"$schema\": \"http://json-schema.org/draft-07/schema#\",";

	/** The Constant SCHEMA_BUILDER_EXCEPTION. */
	public static final String SCHEMA_BUILDER_EXCEPTION =
			"Exception while generating request schema for DatabaseConnector{0}";

	/** The Constant CONNECTION_FAILED_ERROR */
	public static final String CONNECTION_FAILED_ERROR = "Connection failed, please check connection details!";

	/** The Constant CLASSNAME. */
	public static final String CLASSNAME = "className";

	/** The Constant USERNAME. */
	public static final String USERNAME = "username";

	/** The Constant PASS. */
	public static final String PASS = "password";

	/** The Constant SCHEMANAME. */
	public static final String SCHEMA_NAME = "schemaName";

	/** The Constant URL. */
	public static final String URL = "url";

	/** The Constant TABLE. */
	public static final String TABLE = "TABLE";

	/** The Constant TABLE_NAME. */
	public static final String TABLE_NAME = "TABLE_NAME";

	/** The Constant BATCH_COUNT. */
	public static final String BATCH_COUNT = "batchCount";

	/** The Constant REMAINING_BATCH_RECORDS. */
	public static final String REMAINING_BATCH_RECORDS = " Total Number of remaining records in the batch: ";

	/** The Constant COMMIT_OPTION. */
	public static final String COMMIT_OPTION = "CommitOption";

	/** The Constant COMMIT_BY_ROWS. */
	public static final String COMMIT_BY_ROWS = "Commit By Rows";

	/** The Constant COMMIT_BY_PROFILE. */
	public static final String COMMIT_BY_PROFILE = "Commit By Profile";

	/** The Constant BATCH_NUM. */
	public static final String BATCH_NUM = " Batch Number: ";

	/** The Constant BATCH_RECORDS. */
	public static final String BATCH_RECORDS = " Total Number of records in the batch: ";

	/** The Constant GET_TYPE. */
	public static final String GET_TYPE = "GetType";

	/** The Constant DELETE_TYPE. */
	public static final String DELETE_TYPE = "DeleteType";

	/** The Constant INSERTION_TYPE. */
	public static final String INSERTION_TYPE = "InsertionType";
	
	/** The Constant IN_CLAUSE. */
	public static final String IN_CLAUSE = "INClause";

	/** The Constant MAX_ROWS. */
	public static final String MAX_ROWS = "maxRows";

	/** The Constant LINK_ELEMENT. */
	public static final String LINK_ELEMENT = "linkElement";

	/** The Constant GROUP_BY. */
	public static final String GROUP_BY = " group by ";

	/** The Constant WHERE. */
	public static final String WHERE = " where ";

	/** The Constant AND. */
	public static final String AND = " and";

	/** The Constant PROCEDURE_NAME. */
	public static final String PROCEDURE_NAME = "PROCEDURE_NAME";

	/** The Constant GET. */
	public static final String GET = "GET";

	/** The Constant TYPE. */
	public static final String TYPE = "Type";

	/** The Constant DELETE. */
	public static final String DELETE = "DELETE";

	/** The Constant DELETE_QUERY. */
	public static final String DELETE_QUERY = "DELETE FROM ";

	/** The Constant COLUMN_NAME. */
	public static final String COLUMN_NAME = "COLUMN_NAME";

	/** The Constant MSSQLSERVER. */
	public static final String MSSQLSERVER = "Microsoft SQL Server";

	/** The Constant DATA_TYPE. */
	public static final String DATA_TYPE = "DATA_TYPE";

	/** The Constant TYPE_NAME. */
	public static final String TYPE_NAME = "TYPE_NAME";
	
	/** The Constant UNKNOWN_DATATYPE. */
	public static final String UNKNOWN_DATATYPE = "1111";

	/** The Constant EXEC. */
	public static final String EXEC = "EXEC ";

	/** The Constant FAILED_BATCH_NUM. */
	public static final String FAILED_BATCH_NUM = "Failed Batch number: ";
	
	/** The Constant SQL_QUERY. */
	public static final String SQL_QUERY = "SQLQuery";
	
	/** The Constant NON_UNIQUE. */
	public static final String NON_UNIQUE = "NON_UNIQUE";
	
	/** The Constant FAILED_BATCH_RECORDS. */
	public static final String FAILED_BATCH_RECORDS="No of records in Failed batch: ";
	
	/** The Constant INPUT_ERROR. */
	public static final String INPUT_ERROR = "Please check the input data!!";
	
	/** The Constant COMMA. */
	public static final String COMMA = ",";
	
	/** The Constant SINGLE_QUOTE. */
	public static final String SINGLE_QUOTE = "'";
	
	/** The Constant PARAM. */
	public static final String PARAM = "?,";
	
	/** The Constant DOUBLE_QUOTE. */
	public static final String DOUBLE_QUOTE = "\"";
	
	/** The Constant ORACLE. */
	public static final String ORACLE = "Oracle";
	
	/** The Constant POSTGRESQL. */
	public static final String POSTGRESQL = "PostgreSQL";
	
	/** The Constant MSSQL. */
	public static final String MSSQL = "Microsoft SQL Server";
	
	/** The Constant MYSQL. */
	public static final String MYSQL = "MySQL";
	
	/** The Constant DOT. */
	public static final String DOT = ".";
	
	/** The Constant OPEN_IN. */
	public static final String OPEN_IN ="IN\\(";
	
	/** The Constant OPEN_EXEC. */
	public static final String OPEN_EXEC = "EXEC(";
	
	/** The Constant IDENTITY_QUERY. */
	public static final String IDENTITY_QUERY =
			"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE IDENTITY_COLUMN = 'YES' AND TABLE_NAME='";
	
	/** The Constant DECIMAL_DIGITS. */
	public static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
	
	/** The Constant VIEWS. */
	public static final String VIEWS = "VIEW";
	
	/** The Constant WHERE_PARAM. */
	public static final String WHERE_PARAM = "param_where";
	
	/** The Constant ORDER_BY_PARAM. */
	public static final String ORDER_BY_PARAM = "param_orderby";
	
	/** The Constant Dynamic. */
	public static final String DYNAMIC = "dynamic";
	
	/** The Constant PROCEDURE_NAME. */
	public static final String PROCEDURE_PACKAGE_NAME = "PROCEDURE_CAT";	
	
	/** The Constant CAPS_IN. */
	public static final String CAPS_IN= "IN\\s*\\(";
	
	/** The Constant SMALL_IN. */
	public static final String SMALL_IN = "in\\s*\\(";
	
	/** The Constant INVALID_ERROR. */
	public static final String INVALID_ERROR = "Please enter valid Date format : ";
	
	/** The Constant MSSQL_DEFAULT_SCHEMA. */
	public static final String MSSQL_DEFAULT_SCHEMA = "dbo";
	
	/** The Constant INDEX_NAME. */
	public static final String INDEX_NAME = "INDEX_NAME";

	/** The Constant MAX_ROW_NOT_PARSABLE. */
	public static final String MAX_ROW_NOT_PARSABLE = "Input passed for maxRow property is not parsable : ";

	/** The Constant PROCEDURE_NAME_PATTERN. */
	public static final String PROCEDURE_NAME_PATTERN = "procedureNamePattern";

	/** The Constant for Success Message. */
	public static final String SUCCESSFUL_EXECUTION_MESSAGE = "Executed Successfully";

	/** The Constant OPERATION_TYPE_NOT_SUPPORTED. */
	public static final String OPERATION_TYPE_NOT_SUPPORTED = "This operation type is not supported:";

	/** The Constant IS_AUTOINCREMENT. */
	public static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";

	/** The Constant YES. */
	public static final String YES = "yes";

	/** The Constant TRUE. */
	public static final String TRUE = "true";

	/** The Constant ONE. */
	public static final String ONE = "1";

	public static final String MAX_FIELD_SIZE = "maxFieldSize";

	public static final String FETCH_SIZE = "fetchSize";

	public static final String BATCH_COUNT_CANNOT_BE_NEGATIVE = "Batch count cannot be negative!!";

	/** The Constant COLUMN_NAMES. */
	public static final String COLUMN_NAMES_KEY = "columnNames";

	/** The Constant REF_CURSOR. */
	public static final String REF_CURSOR = "REF CURSOR";

	/** The Constant ARRAY. */
	public static final String ARRAY = "array";

	private DatabaseConnectorConstants() {
		// No instances required.
	}
}