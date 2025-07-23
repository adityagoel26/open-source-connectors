// Copyright (c) 2025 Boomi, LP
package com.boomi.snowflake.wrappers;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.snowflake.SnowflakeBrowser;
import com.boomi.snowflake.util.ConnectionProperties.ConnectionGetter;
import com.boomi.snowflake.util.ConnectionTimeFormat;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.SnowflakeDataTypeConstants;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.Args;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.TempOutputStream;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeStatement;
import net.snowflake.client.jdbc.internal.joda.time.DateTime;
import net.snowflake.client.jdbc.internal.joda.time.format.DateTimeFormat;
import net.snowflake.client.jdbc.internal.joda.time.format.DateTimeFormatter;
import net.snowflake.client.jdbc.internal.net.minidev.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.logging.Logger;


/**
 * The Class SnowflakeWrapper.
 *
 * @author Vanangudi,S
 */
public class SnowflakeWrapper {

	/** The Constant LOG. */
	private static final Logger LOG = LogUtil.getLogger(SnowflakeWrapper.class);
	/** The Constant MATCHING_LIKE_OPERATOR. */
	protected static final String MATCHING_LIKE_OPERATOR = " LIKE ";
	/** The Constant MATCHING_EQUAL_OPERATOR. */
	protected static final String MATCHING_EQUAL_OPERATOR = " = ";
	/** The Constant COMMA_DELIMITER. */
	protected static final String COMMA_DELIMITER = " , ";
	/** The Constant AND_DELIMITER. */
	protected static final String AND_DELIMITER = " AND ";
	/** The Constant QUESTION_MARK. */
	protected static final String QUESTION_MARK = " ? ";
	/** The Constant SQL_COMMAND_WHERE. */
	protected static final String SQL_COMMAND_WHERE = " WHERE ";
	/** The Constant SQL_COMMAND_ORDER_BY. */
	protected static final String SQL_COMMAND_ORDER_BY = " ORDER BY ";
	/** The Constant SQL_COMMAND_SELECT. */
	protected static final String SQL_COMMAND_SELECT = "SELECT ";
	/** The Constant SQL_COMMAND_FROM. */
	protected static final String SQL_COMMAND_FROM = " FROM ";
	/** The Constant SQL_COMMAND_DELETE. */
	protected static final String SQL_COMMAND_DELETE = "DELETE ";
	/** The Constant SQL_COMMAND_TRUNCATE. */
	protected static final String SQL_COMMAND_TRUNCATE = "TRUNCATE TABLE if exists ";
	/** The Constant SQL_COMMAND_UPDATE. */
	protected static final String SQL_COMMAND_UPDATE = "UPDATE ";
	/** The Constant SQL_COMMAND_INSERT. */
	protected static final String SQL_COMMAND_INSERT = "INSERT INTO ";
	/** The Constant SQL_COMMAND_VALUES. */
	protected static final String SQL_COMMAND_VALUES = " VALUES ";
	/** The Constant SQL_COMMAND_SET. */
	protected static final String SQL_COMMAND_SET = " SET ";
	/** The Constant SQL_COMMAND_LIMIT. */
	protected static final String SQL_COMMAND_LIMIT = " LIMIT ";
	/** The Constant SQL_COMMAND_OFFSET. */
	protected static final String SQL_COMMAND_OFFSET = " OFFSET ";
	/** The Constant DEFAULT_DATETIME_FORMAT. */
	protected static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS";
	/** The Constant SQL_PARAMETER_MULTI_STATEMENT. */
	protected static final String SQL_PARAMETER_MULTI_STATEMENT = "MULTI_STATEMENT_COUNT";
	/** The Constant DOUBLE_QUOTES. */
	protected static final String DOUBLE_QUOTES = "\"";
	/** The Constant UPDATE_COUNT_JSON_OBJECT. */
	private static final String UPDATE_COUNT_JSON_OBJECT = "[{\"UpdateCount\":\"%s\"}]";
	/** The Constant STR_SQL. */
	private static final String STR_SQL = "SQL: %s";
	/** The Table Name. */
	protected String _tableName;
	/** The Prepared Statement Object. */
	protected PreparedStatement _preparedStatement;
	/** The Logger object. */
	protected Logger _processLogger;
	/** The Current Statement indicator. */
	protected int _currentStatement;
	/** The Connection Time Format object. */
	private ConnectionTimeFormat _connectionTimeFormat;
	/** The Result Set object. */
	private ResultSet _resultSet;
	/** The Update Count. */
	private Integer _updateCount;
	/** The Snow SQL Batch Count. */
	private int _snowSQlBatchCount;
	/** The Batched Count. */
	private int _batchedCount;
	/** The Current Batch Number. */
	private int _batchNumber;
	/** The Temp Output Stream object. */
	private TempOutputStream _tmpStream;
	/** The Data Object Defination. */
	protected SortedMap<String, String> dataObjectDefinition;
	/** The Filter Object Defination. */
	protected SortedMap<String, String> filterObjectDefinition;
	/** The Connection Getter object. */
	protected ConnectionGetter _getter;
	/** The truncate Prepared Statement Object. */
	private PreparedStatement _truncateStatement;
	/** The Constant AUTO_FORMAT. */
	protected static final String AUTO_FORMAT = "AUTO";
	private static final String PROPERTY = "property";
	private static final String ARGUMENTS = "arguments";
	private static final String OPERATOR = "operator";
	private final Map<String, Set<String>> _truncatedMap = new HashMap<>();

	/**
	 *  Instantiates a new Snowflake Wrapper
	 *  
	 * @param Getter    			contains the connection getter properties
	 * @param connectionTimeFormat  Time format of the connection
	 * @param logger     			Logger properties
	 * @param tableName 			table name that will be operated on              
	 */
	public SnowflakeWrapper(ConnectionGetter Getter, ConnectionTimeFormat connectionTimeFormat, Logger logger,
			String tableName) {
		LOG.entering(this.getClass().getCanonicalName(), "SnowflakeWrapper()");
		dataObjectDefinition = new TreeMap<>();
		filterObjectDefinition = new TreeMap<>();
		_getter = Getter;
		_batchNumber = 1;
		_connectionTimeFormat = connectionTimeFormat;
		_processLogger = logger;
		_tmpStream = null;
		
		//added the below code to fix the backward compatibility issue on import object ID
		if(tableName != null && tableName.lastIndexOf("\".\"") != -1) {
			_tableName = tableName.substring(tableName.lastIndexOf("\".\"")+2);
		}
		else {
			_tableName = tableName;
		}
		_batchedCount = 0;
		_snowSQlBatchCount = 0;
		_preparedStatement = null;
		_resultSet = null;
		_updateCount = null;
		_currentStatement = -1;
		_processLogger.fine("Database connection initialized successfully");
	}

	/**
	 * helper function used to get a prepared Statement
	 * 
	 * @param sqlCommand contains the SQL command
	 * @return PreparedStatement from the connection
	 */
	public PreparedStatement createPreparedStatement(String sqlCommand) {
		LOG.entering(this.getClass().getCanonicalName(), "createPreparedStatement()");
		_processLogger.fine("Perparing SQL statement");
		_processLogger.fine(() -> String.format(STR_SQL, sqlCommand));
		try {
			return _getter.getConnection(_processLogger).prepareStatement(sqlCommand);
		} catch (SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.BUILDING_SQL_ERROR, e);
		}
	}

	/**
	 * helper function used to get a prepared Statement
	 *
	 * @param sqlCommand        contains the SQL command
	 * @param dynamicProperties contains DynamicOperationProperties
	 * @return PreparedStatement from the connection
	 */
	public PreparedStatement createPreparedStatement(String sqlCommand, DynamicPropertyMap dynamicProperties) {
		LOG.entering(this.getClass().getCanonicalName(), "createPreparedStatement()");
		_processLogger.fine(String.format("Preparing SQL statement with database - %s and schema %s",
				dynamicProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
				dynamicProperties.getProperty(SnowflakeOverrideConstants.SCHEMA)));
		_processLogger.fine(() -> String.format(STR_SQL, sqlCommand));
		try {
			return _getter.getConnection(_processLogger,dynamicProperties).prepareStatement(sqlCommand);
		} catch (SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.BUILDING_SQL_ERROR, e);
		}
	}

	/**
	 * helper function to close prepared statement
	 * 
	 * @param preparedStatement prepared statement to be closed
	 */
	protected void closePreparedStatement(PreparedStatement preparedStatement) {
		LOG.entering(this.getClass().getCanonicalName(), "closePreparedStatement()");
		try {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		} catch (SQLException e) {
			throw new ConnectorException("Errors occurred while closing SQL statement", e);
		}
	}

	/**
	 * helper function used to execute prepared statement
	 * 
	 * @param preparedStatement prepared statement to be executed
	 * @return ResultSet containing the returned data
	 */
	protected ResultSet executePreparedStatement(PreparedStatement preparedStatement) {
		LOG.entering(this.getClass().getCanonicalName(), "executePreparedStatement()");
		try {
			return preparedStatement.executeQuery();
		} catch (SQLException e) {
			throw new ConnectorException("Errors occurred while building or executing SQL statement", e);
		}
	}

	/**
	 * get private prepared statement
	 * 
	 * @return PreparedStatement
	 */
	public PreparedStatement getPreparedStatement() {
		LOG.entering(this.getClass().getCanonicalName(), "getPreparedStatement()");
		return _preparedStatement;
	}

	/**
	 * sets the prepared statement
	 * 
	 * @param newVal is the new value of the prepared statement
	 */
	public void setPreparedStatement(PreparedStatement newVal) {
		LOG.entering(this.getClass().getCanonicalName(), "setPreparedStatement()");
		_preparedStatement = newVal;
	}

	/**
	 * getter for the number of batched commands
	 * 
	 * @return Integer contains the number of current batched commands
	 */
	public int getBatchedCount() {
		LOG.entering(this.getClass().getCanonicalName(), "getBatchedCount()");
		return _batchedCount;
	}

	/**
	 * Sets this connection's auto-commit mode to the given state
	 * 
	 * @param autoCommit <code>true</code> to enable auto-commit mode
	 *                   <code>false</code> to disable auto-commit mode
	 */
	public void setAutoCommit(boolean autoCommit) {
		LOG.entering(this.getClass().getCanonicalName(), "setAutoCommit()");
		try {
			_getter.getConnection(_processLogger).setAutoCommit(autoCommit);
		} catch (SQLException e) {
			throw new ConnectorException("Failed to set connection auto commit.", e);
		}
	}

	/**
	 * Builds the parameters part of a SQL command it constructs a string in this
	 * format: "[key][matchingOperator][?][delimiter][key][matchingOperator][?]"
	 * 
	 * @param filterSortedMap  Map Object where Keys are used and values are ignored
	 * @param matchingOperator String appended after each key
	 * @param delimiter        String inserted between multiple parameters to
	 *                         connect them. e.g: "AND" or "OR"
	 * @return string used in SQL commands
	 */
	private String sqlConstructParameters(SortedMap<String, String> filterSortedMap,
			String matchingOperator, String delimiter) {
		LOG.entering(this.getClass().getCanonicalName(), "sqlConstructParameters()");
		StringJoiner result = new StringJoiner(StringUtil.EMPTY_STRING);
		for (String keyStr : filterSortedMap.keySet()) {
			if (result.length() != 0) {
				result.add(delimiter);
			}

			result.add(DOUBLE_QUOTES)
					.add(keyStr)
					.add(DOUBLE_QUOTES)
					.add(matchingOperator)
					.add(QUESTION_MARK);
		}
		return result.toString();
	}

	private String sqlConstructQueryParameters(List<Map<String, String>> expressionList) {
		LOG.entering(this.getClass().getCanonicalName(), "sqlConstructParameters()");
		StringJoiner result = new StringJoiner(StringUtil.EMPTY_STRING);

		for (Map<String,String> map: expressionList){
			if (result.length() != 0) {
				result.add(SnowflakeWrapper.AND_DELIMITER);
			}

			result.add(DOUBLE_QUOTES)
					.add(map.get(PROPERTY))
					.add(DOUBLE_QUOTES)
					.add(map.get(OPERATOR) == null ? SnowflakeWrapper.MATCHING_LIKE_OPERATOR : map.get(OPERATOR))
					.add(QUESTION_MARK);
		}
		return result.toString();
	}

	/**
	 * Builds the field part of a SQL command it constructs a string in this format:
	 * "[key],[key],[key]"
	 * @param input
	 * @param emptyFieldSelection
	 * @param batchSize
	 * @return string used in SQL commands
	 */
	private String sqlConstructFields(SortedMap<String, String> input, String emptyFieldSelection, Long batchSize) {
		LOG.entering(this.getClass().getCanonicalName(), "sqlConstructFields()");
		StringBuilder result = new StringBuilder();
		if (emptyFieldSelection.equalsIgnoreCase(SnowflakeDataTypeConstants.DEFAULT_SELECTION) && batchSize == 1) {
			for (String keyStr : input.keySet()) {
				buildResult(result, keyStr);
			}
		} else {
			for (String keyStr : dataObjectDefinition.keySet()) {
				buildResult(result, keyStr);
			}
		}
		return result.toString();
	}

	/**
	 * Builds the result.
	 *
	 * @param result the result
	 * @param keyStr the key str
	 */
	private void buildResult(StringBuilder result, String keyStr) {
		if (result.length() != 0) {
			result.append(COMMA_DELIMITER);
		}
		result.append(DOUBLE_QUOTES).append(keyStr).append(DOUBLE_QUOTES);
	}

	/**
	 * construct WHERE part of a SQL command
	 * 
	 * @param filterObj JSON object containing the filter data
	 * @return a string that when concatenated to any SQL command applies where condition
	 */
	protected String sqlConstructWhereClause(SortedMap<String, String> filterObj) {
		LOG.entering(this.getClass().getCanonicalName(), "sqlConstructWhereClause()");
		if (filterObj.isEmpty()) {
			return "";
		}
		return SQL_COMMAND_WHERE + sqlConstructParameters(filterObj, MATCHING_LIKE_OPERATOR, AND_DELIMITER);
	}

	protected String sqlConstructQueryWhereClause(List<Map<String, String>> expressionList) {
		LOG.entering(this.getClass().getCanonicalName(), "sqlConstructQueryWhereClause()");
		return expressionList.isEmpty() ? StringUtil.EMPTY_STRING
				: (SQL_COMMAND_WHERE + sqlConstructQueryParameters(expressionList));
	}

	/**
	 * Constructs the SET part of an update SQL command
	 * 
	 * @param filterObj
	 * @return Sql Parameter String
	 */
	private String sqlConstructUpdateValues(SortedMap<String, String> filterObj) {
		LOG.entering(this.getClass().getCanonicalName(), "sqlConstructUpdateValues()");
		return sqlConstructParameters(filterObj, MATCHING_EQUAL_OPERATOR, COMMA_DELIMITER);
	}

	/**
	 * Read the Input Stream from the Result Set
	 * @return  input stream that contains all data of result set
	 */
	public InputStream readFromResultSet() {
		try {
			if(!_resultSet.next()) {
				return StreamUtil.EMPTY_STREAM;
			}
		}catch(SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.FETCH_DATA_ERROR, e);
		}
		return resultSetToStreamBatchWise(_resultSet,0);
	}
	
	/**
	 * executes prepared statement
	 */
	public void executePreparedStatement() {
		try {
			_resultSet = _preparedStatement.executeQuery();
		} catch (SQLException e) {
			throw new ConnectorException("Unable to execute prepared statement", e);
		}
	}
	
	/**
	 * convert all of the Result Set into a single stream containing JSON Strings separated by new lines
	 * @param resultSet from executed statement
	 * @param batchSize batchSize for the Operation
	 * @return input stream that contains all data of result set
	 */
	protected InputStream resultSetToStreamBatchWise(final ResultSet resultSet, final long batchSize){
		/**
		 * The Class Stream Combiner
		 * @author s.vanangudi
		 *
		 */
		class StreamCombiner extends InputStream{
			
			private static final char SQUARE_BRACKET_OPENING = '[';
			private static final char SQUARE_BRACKET_CLOSING = ']';
			private static final char JSON_OBJECT_DELIMITER = ',';
			
			long currentDoc;
			InputStream curInputStream;
			boolean firstChar;
			boolean lastChar;
			
			/**
			 * Initiates the Stream Combiner
			 */
			StreamCombiner(){
				firstChar = true;
				lastChar = false;
				currentDoc = 1;
				curInputStream = resultSetToStreams(resultSet);
			}

			@Override
			public int read() throws IOException {
				if(lastChar){
					return -1;
				}
				if(firstChar) {
					firstChar = false;
					return Integer.valueOf(SQUARE_BRACKET_OPENING);
				}
				int _nxt = curInputStream.read();
				
				if(_nxt == -1) {
					IOUtil.closeQuietly(curInputStream);
					try {
						if(currentDoc == batchSize || !resultSet.next()) {
							lastChar = true;
							return Integer.valueOf(SQUARE_BRACKET_CLOSING);
						}
						currentDoc++;
						curInputStream = resultSetToStreams(resultSet);
						return Integer.valueOf(JSON_OBJECT_DELIMITER);
					} catch (SQLException e) {
						throw new ConnectorException(SnowflakeDataTypeConstants.FETCH_DATA_ERROR, e);
					}
					
				}
				return _nxt;
			}
			
		}
		return new StreamCombiner();
	}
	
	/**
	 *  Retrieves single row from Database
	 * @param resultSet from SQL
	 * @return input stream containing the JSON string
	 */
	// Sonar issue: java:S3776, reduce cognitive complexity
	protected InputStream resultSetToStreams(ResultSet resultSet) {
		IOUtil.closeQuietly(_tmpStream);
		_tmpStream = new TempOutputStream();
		/**
		 * here we are using TemoOutputStream to assemble row in disk if it's size exceeded 1 MB,
		 *  Knowing that it's very unlikely that a row is going to be over 1MB.
		 * 
		 */
		try {
			_tmpStream.write("{".getBytes());
			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			int columnCount = resultSetMetaData.getColumnCount();
			for (int colIdx = 1; colIdx <= columnCount; colIdx++) {
				if(colIdx != 1) {
					_tmpStream.write(",".getBytes());
				}
				String key = resultSetMetaData.getColumnName(colIdx);
				String value = "";
				boolean booleanvalue = false;
				BigDecimal bigValue = null;
				Double doubleValue = null;
				boolean floatConstant = false;
				
				if(resultSet.getObject(colIdx) != null) {
					int colType = resultSetMetaData.getColumnType(colIdx);
					if (colType == java.sql.Types.TIME
							|| colType == java.sql.Types.TIME_WITH_TIMEZONE) {
						if(_connectionTimeFormat.getTimeFormat().equalsIgnoreCase(AUTO_FORMAT)) {
							value = resultSet.getTime(colIdx).toString();
						}else {
							SimpleDateFormat formatter = new SimpleDateFormat(_connectionTimeFormat.getTimeFormat());
							value = formatter.format(resultSet.getTime(colIdx));
						}
					} else if (colType == java.sql.Types.TIMESTAMP
							|| colType == java.sql.Types.TIMESTAMP_WITH_TIMEZONE) {
						if(_connectionTimeFormat.getDateTimeFormat().equalsIgnoreCase(AUTO_FORMAT)) {
							value = resultSet.getTimestamp(colIdx).toString();
						}else {
							DateTimeFormatter formatter = DateTimeFormat
								.forPattern(DEFAULT_DATETIME_FORMAT);
							DateTime dateTime = formatter
								.parseDateTime(resultSet.getTimestamp(colIdx).toString());
							formatter = DateTimeFormat.forPattern(_connectionTimeFormat.getDateTimeFormat());
							value = dateTime.toString(formatter);
						}
					} else if (colType == java.sql.Types.DATE) {
						if(_connectionTimeFormat.getDateFormat().equalsIgnoreCase(AUTO_FORMAT)) {
							value = resultSet.getDate(colIdx).toString();
						}else {
							SimpleDateFormat formatter = new SimpleDateFormat(_connectionTimeFormat.getDateFormat());
							value = formatter.format(resultSet.getDate(colIdx));
						}
					}else if (colType == java.sql.Types.BIGINT) {
						bigValue = resultSet.getBigDecimal(colIdx);
					}else if (colType == java.sql.Types.BOOLEAN) {
						booleanvalue = resultSet.getBoolean(colIdx);
					}else if (colType == java.sql.Types.DOUBLE) {
						Double val = resultSet.getDouble(colIdx);
						if(Double.isNaN(val)){
							floatConstant = true;
							value = Double.toString(Double.NaN);
						}
						else if(Double.isInfinite(val) && val > 0){
							floatConstant = true;
							value = Double.toString(Double.POSITIVE_INFINITY);
						}
						else if(Double.isInfinite(val) && val < 0){
							floatConstant = true;
							value = Double.toString(Double.NEGATIVE_INFINITY);
						}
						else {
							doubleValue = resultSet.getDouble(colIdx);
						}
					}else {
						value = resultSet.getString(colIdx);
						/**
						 * here we cannot stream data because it's not supported in Snowflake jdbc driver
						 * also, we have to retrieve data as a string because we need to do processing on it
						 * and making it valid JSON string
						 * so we cannot do resultSet.getBytes(colIdx)
						 */
					}
				}
				JSONObject jsonObj = new JSONObject();
				if(resultSetMetaData.getColumnType(colIdx) == java.sql.Types.BIGINT) {
					jsonObj.put(key, bigValue);
				}else if(resultSetMetaData.getColumnType(colIdx) == java.sql.Types.BOOLEAN) {
					jsonObj.put(key, booleanvalue);
				}else if(resultSetMetaData.getColumnType(colIdx) == java.sql.Types.DOUBLE && !floatConstant) {
					jsonObj.put(key, doubleValue);
				}else {
					jsonObj.put(key, value);
				}
				String jsonStr = jsonObj.toString();
				jsonObj.clear();
				_tmpStream.write(jsonStr.substring(1, jsonStr.length()  - 1).getBytes(StandardCharsets.UTF_8));
			}
			_tmpStream.write("}".getBytes());
			return _tmpStream.toInputStream();
		} catch (SQLException e) {
			throw new ConnectorException("Unable to parse result set.", e);
		} catch (IOException e) {
			throw new ConnectorException("Unable to write to temp stream.", e);
		}
	}

	/**
	 * executes multi-statement SQL commands and retrieves it's data
	 * @param sqlCommands        string that contains SQL commands separated by ;
	 * @param numberOfStatements number of SQL commands inside the string
	 *                           sqlCOmmands
	 * @param dynamicPropertyMap A map containing dynamic properties such as databas and schema, used for overriding
	 *                           connection properties during execution.
	 */
	public void executeMultiStatement(String sqlCommands, Long numberOfStatements, DynamicPropertyMap dynamicPropertyMap)
	{
		LOG.entering(this.getClass().getCanonicalName(), "executeMultiStatemnt()");

		_processLogger
				.fine(() -> String.format("Executing Multi-Statement containing %d Statement(s)", numberOfStatements));
		_processLogger.fine(() -> String.format(STR_SQL, sqlCommands));
		try {

				_getter.getStatement(dynamicPropertyMap).unwrap(SnowflakeStatement.class)
						.setParameter(SQL_PARAMETER_MULTI_STATEMENT,
						numberOfStatements);
				_getter.getStatement(dynamicPropertyMap).execute(sqlCommands);

			_currentStatement = -1;
		} catch (SQLException e) {
			throw new ConnectorException("Execution of multi-statement Failed: ", e);
		}
	}
	
	/**
	 * executes multi-statement SQL commands and retrieves it's data when batch size is greater than 1
	 * 
	 * @param sqlCommands        string that contains SQL commands separated by ;
	 * @param batchSize			 Batch Size of the operation
	 * @param lastExecution		 Determines if this the last batch to execute
	 * @param numberOfScripts number of SQL commands inside the string
	 *                           sqlCOmmands
	 * @throws SQLException 
	 */
	public void executeMultiStatementBatch(String sqlCommands, Long batchSize, boolean lastExecution,
			Long numberOfScripts) throws SQLException {
		try {
			_getter.getStatement().addBatch(sqlCommands);
			++_snowSQlBatchCount;
		} catch (SQLException e) {
			throw new ConnectorException("Unable to add batch " + _batchNumber + ": ", e);
		}

		if (_snowSQlBatchCount >= batchSize || lastExecution) {
			_snowSQlBatchCount = 0;
			try {
				_getter.getStatement().unwrap(SnowflakeStatement.class).setParameter(SQL_PARAMETER_MULTI_STATEMENT,
						numberOfScripts);
				_getter.getStatement().executeBatch();
			} catch (BatchUpdateException e) {
				if (!ErrorCode.UPDATE_FIRST_RESULT_NOT_UPDATE_COUNT.getMessageCode().equals(e.getErrorCode())
						|| !ErrorCode.UPDATE_FIRST_RESULT_NOT_UPDATE_COUNT.getSqlState().equals(e.getSQLState())) {
					throw new ConnectorException("[BatchUpdateException] Unable to execute batch " + _batchNumber + ": ", e);
				}
			} catch (SQLException e) {
				throw new ConnectorException("[SQLException] Unable to execute batch " + _batchNumber + ": ", e);
			} finally {
				_batchNumber++;
				if (!lastExecution) {
					_getter.getConnection(_processLogger).commit();
					_getter.getStatement().clearBatch();
				}
			}
		}
	}
	
	/**
	 * constructs output from a single statement from the SQL script
	 * @return input stream containing array of JSON elements
	 */
	public InputStream getResultFromNextStatement() {
		try {
			if(_resultSet == null && _updateCount == null) {
				_resultSet = _getter.getStatement().getResultSet();
				_updateCount = _getter.getStatement().getUpdateCount();
			}else {
				if(_resultSet != null) {
					_resultSet.close();
				}
				
				if(!_getter.getStatement().getMoreResults()
                        && (_updateCount = _getter.getStatement().getUpdateCount()) == -1) {
					_resultSet = null;
					_updateCount = null;
					return null;
				}
					
				_resultSet = _getter.getStatement().getResultSet();
			}
			
		}catch(SQLException e) {
			throw new ConnectorException("Unable to retrieve data from Multi - Statement: ", e);
		}
		if(_resultSet != null) {
			try {
				if(!_resultSet.next()) {
					return StreamUtil.EMPTY_STREAM;
				}
			}catch(SQLException e) {
				throw new ConnectorException(SnowflakeDataTypeConstants.FETCH_DATA_ERROR, e);
			}
			return resultSetToStreamBatchWise(_resultSet,0);
		}
		
		if(_updateCount != -1) {
			return new ByteArrayInputStream(String.format(UPDATE_COUNT_JSON_OBJECT,
					String.valueOf(_updateCount)).getBytes(StandardCharsets.UTF_8));
		}
		
		return null;
	}

	/**
	 * increments the cursor and retrieves a row from db
	 * @param dynamicPropertyMap The dynamic properties containing overrides for the database and schema.
	 * @return stream containing JSON string represents the next row
	 * @throws SQLException
	 */
	protected InputStream getNextRowFromMultiStatement(DynamicPropertyMap dynamicPropertyMap) throws SQLException {
		do {
			if (_resultSet == null) {
				// move to the next statement
				_resultSet = _getter.getStatement(dynamicPropertyMap).getResultSet();
				++_currentStatement;
			}
			if (_resultSet != null && _resultSet.next()) {
				// current statement has data to be retrieved
				return resultSetToStreams(_resultSet);
			}
			if(_resultSet != null) {
				_resultSet.close();
			}
			_resultSet = null;
		} while (_getter.getStatement(dynamicPropertyMap).getMoreResults());
		return null;
	}

	/**
	 * Construct a parameterized Delete Statement with WHERE columns from
	 * filterJSONObj
	 * 
	 * @param filterObj the Object to extract WHERE column names from
	 * @return PreparedStatement the constructed delete statement with parameters
	 */
	public PreparedStatement constructDeleteStatement(SortedMap<String, String> filterObj) {
		LOG.entering(this.getClass().getCanonicalName(), "executeDeleteRows()");
		fillObjectDefination(filterObjectDefinition, filterObj);
		String sqlCommand = SQL_COMMAND_DELETE + SQL_COMMAND_FROM + _tableName
				+ sqlConstructWhereClause(filterObj);
		_processLogger.fine("Constructing Delete Statement");
		_processLogger.fine(() -> String.format(STR_SQL, sqlCommand));
		return createPreparedStatement(sqlCommand);
	}

	/**
	 * Construct a Truncate Table Statement
	 *
	 * @return PreparedStatement the constructed truncate table statement
	 */
	private PreparedStatement constructTruncateStatement() {
		String sqlCommand = SQL_COMMAND_TRUNCATE + _tableName;
		return createPreparedStatement(sqlCommand);
	}

	/**
	 * truncates table
	 */
	private void truncate() {
		_processLogger.fine("Executing truncate table statement.");
		_truncateStatement= constructTruncateStatement();
		executeTruncateQuery();
	}

	/**
	 * Truncates data if the specified schema has not been truncated for the given database.
	 *
	 * @param dynamicProperties a {@link DynamicPropertyMap} containing database and schema properties.
	 */
	public void truncateTableIfNotDone(DynamicPropertyMap dynamicProperties){
		String databaseName = dynamicProperties.getProperty(SnowflakeOverrideConstants.DATABASE);
		String schemaName = dynamicProperties.getProperty(SnowflakeOverrideConstants.SCHEMA);
		Set<String> truncatedSchemas = _truncatedMap.computeIfAbsent(databaseName, k -> new HashSet<>());
		if (!truncatedSchemas.contains(schemaName)) {
			truncate();
			truncatedSchemas.add(schemaName);
		}
	}

	/**
	 * Constructs a parameterized SQL `INSERT` statement using the provided parameters.
	 *
	 * @param input               a {@link SortedMap} of column names and values for the `INSERT` statement.
	 * @param emptyFieldSelection a {@link String} indicating how to handle empty fields.
	 * @param metadata            a {@link SortedMap} of table metadata used for constructing the statement.
	 * @param batchSize           a {@link Long} specifying the number of records for batch operations.
	 * @param dynamicProperty     the {@link  DynamicPropertyMap} containing new values for DATABASE and SCHEMA
	 * @return a {@link PreparedStatement} with the constructed SQL `INSERT` statement.
	 * @throws ConnectorException if there is an issue with the input or statement construction.
	 */
	public PreparedStatement constructInsertStatement(SortedMap<String, String> input, String emptyFieldSelection,
			SortedMap<String, String> metadata, Long batchSize, DynamicPropertyMap dynamicProperty) {
		LOG.entering(this.getClass().getCanonicalName(), "constructInsertStatement()");
		dataObjectDefinition = JSONHandler.jsonToSortedMap(
				SnowflakeBrowser.getObjectProperties(_tableName, _getter.getConnection(_processLogger), metadata));
		StringBuilder questionMarks = new StringBuilder();
		if (emptyFieldSelection.equalsIgnoreCase(SnowflakeDataTypeConstants.DEFAULT_SELECTION) && batchSize == 1) {
			if (input.keySet().isEmpty()) {
				throw new ConnectorException(SnowflakeDataTypeConstants.EMPTY_JSON_ERROR);
			} else {
				handleNonEmptyInput(input, questionMarks);
			}
		} else {
			handleNonEmptyInput(dataObjectDefinition, questionMarks);
		}
		String sqlCommand = SQL_COMMAND_INSERT + _tableName + '(' + sqlConstructFields(input, emptyFieldSelection,
				batchSize) + ')' + SQL_COMMAND_VALUES + '(' + questionMarks + ')';
		_processLogger.fine(() -> String.format(STR_SQL, sqlCommand));
		return createPreparedStatement(sqlCommand,dynamicProperty);
	}

	private void handleNonEmptyInput(SortedMap<String, String> input, StringBuilder questionMarks) {
		for (int i = 0; i < input.keySet().size(); ++i) {
			if (questionMarks.length() > 0) {
				questionMarks.append(",");
			}
			questionMarks.append("?");
		}
	}

	/**
	 * Executes a prepared statement
	 * 
	 * @param batchSize the batch size: 1 if no batching applied
	 */
	public void executeQuery(long batchSize) {
		LOG.entering(this.getClass().getCanonicalName(), "executeQuery()");
		_processLogger.fine("Executing SQL Query");
		try {
			if (batchSize == 1) {
				_preparedStatement.executeQuery();
			} else {
				_preparedStatement.executeBatch();
				_getter.getConnection(_processLogger).commit();
				_preparedStatement.clearBatch();
			}
		} catch (SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.SQL_EXECUTION_ERROR, e);
		}
	}

	/**
	 * Executes the SQL truncate query and handles any SQL exceptions.
	 * <p>
	 * Logs method entry and execution details, and wraps SQL exceptions in a {@link ConnectorException}.
	 */
	private void executeTruncateQuery() {
		_processLogger.fine("Executing Truncate Query");
		try {
				_truncateStatement.executeQuery();
		} catch (SQLException e) {
			LOG.entering(this.getClass().getCanonicalName(), "executeTruncateQuery()");
			throw new ConnectorException(SnowflakeDataTypeConstants.SQL_EXECUTION_ERROR, e);
		}
	}

	/**
	 * sends statement to execution if its ready otherwise updates the batch count
	 * 
	 * @param batchSize the batch size: 1 if no batching applied
	 */
	public void executeHandler(long batchSize) {
		LOG.entering(this.getClass().getCanonicalName(), "executeHandler()");
		// execute insert or wait to complete batch
		try {
			if (batchSize == 1) {
				executeQuery(batchSize);
			} else {
				_preparedStatement.addBatch();
				if (++_batchedCount == batchSize) {
					_batchedCount = 0;
					_batchNumber++;
					executeQuery(batchSize);
				}

			}
		} catch (SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.SQL_EXECUTION_ERROR, e);
		}

	}
	
	/**
	 * validates if the input document contained unneeded data
	 * @param inputData sorted map representing the input document
	 * @param objectDef object defination to validate against.
	 */
	private void validateInputAgainstObjectDefinition(SortedMap<String, String> inputData,
			SortedMap<String, String> objectDef) {
		for (String keyStr : inputData.keySet()) {
			if (objectDef.get(keyStr) == null) {
				throw new ConnectorException("Invalid input document, JSON element shouldn't be there: " + keyStr);
			}
		}
	}
	/**
	 *  extracts key sets from src and fill them in dest 
	 * @param dest to be filled with keys
	 * @param src to be extracted
	 */
	protected void fillObjectDefination(SortedMap<String, String> dest,SortedMap<String, String> src) {
		dest.clear();
		for(String keyStr : src.keySet()) {
			dest.put(keyStr, QUESTION_MARK);
		}
	}
	
	/**
	 * Construct a parameterized update Statement with SET columns from dataJSONObj
	 * and WHERE columns from filterJSONObj
	 * 
	 * @param dataJSONObj   the Object to extract SET column names from
	 * @param filterJSONObj the Object to extract WHERE column names from
	 * @return PreparedStatement the constructed insert statement with parameters
	 */
	public PreparedStatement constructUpdateStatement(SortedMap<String, String> dataJSONObj,
			SortedMap<String, String> filterJSONObj) {
		LOG.entering(this.getClass().getCanonicalName(), "constructUpdateStatement()");
		dataObjectDefinition = new TreeMap<>();
		fillObjectDefination(dataObjectDefinition, dataJSONObj);
		fillObjectDefination(filterObjectDefinition, filterJSONObj);
		String sqlCommand = SQL_COMMAND_UPDATE + _tableName + SQL_COMMAND_SET + sqlConstructUpdateValues(dataJSONObj)
				+ sqlConstructWhereClause(filterJSONObj);
		_processLogger.fine("Constructing update SQL Command");
		_processLogger.fine(() -> String.format(STR_SQL, sqlCommand));
		return createPreparedStatement(sqlCommand);
	}
	
	/**
	 * Gets the Current Batch.
	 * @return the current batch number
	 */
	public int getCurrentBatch() {
		return _batchNumber;
	}

	/**
	 * perform DB commit and closes statements
	 * @throws SQLException 
	 */
	public void finish() {
		LOG.entering(this.getClass().getCanonicalName(), "finish()");

		// closing statements properly
		Deque<RuntimeException> exceptionStack = new LinkedList<>();
		try {
			if (_resultSet != null) {
				_resultSet.close();
			}
		} catch (SQLException e) {
			exceptionStack.push(new ConnectorException("Unable to close Result Set", e));
		}
		try {
			if (_preparedStatement != null) {
				_preparedStatement.close();
			}
		} catch (SQLException e) {
			exceptionStack.push(new ConnectorException("Unable to close prepared statement", e));
		}

		if (!exceptionStack.isEmpty()) {
			throw exceptionStack.pop();
		}
	}

	/**
	 * closes JDBC connection
	 * @throws Exception
	 */
	public void close() {
		LOG.entering(this.getClass().getCanonicalName(), "close()");
		IOUtil.closeQuietly(_tmpStream);
		try {
			finish();
		} catch (Exception e) {
			throw e;
		}finally {
			_getter.close();
		}
		_processLogger.fine("Database connection properly closed");
	}
	
	/**
	 * Sets values form JSON Objects into a parameterized statement
	 * 
	 * @param statement       parameterized statement to be filled
	 * @param dataSortedMap   the Map Object contains the values
	 * @param fitlerSortedMap the Map Object contains the filtering values
	 */
	public void fillStatementValuesWithDataType(PreparedStatement statement, SortedMap<String, String> dataSortedMap,
			SortedMap<String, String> fitlerSortedMap, SortedMap<String, String> metadata) {
		validateInputAgainstObjectDefinition(dataSortedMap, dataObjectDefinition);
		validateInputAgainstObjectDefinition(fitlerSortedMap, filterObjectDefinition);
		int index = 1;
		try {
			for (String keyStr : dataObjectDefinition.keySet()) {
				index = setFieldValues(statement, dataSortedMap, metadata, index, keyStr);
			}

			for (String keyStr : filterObjectDefinition.keySet()) {
				index = setStatementString(statement, fitlerSortedMap, index, keyStr);
			}

		} catch (SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.BUILDING_SQL_ERROR, e);
		}
	}

	public void fillStatementValuesWithDataTypeForQuery(PreparedStatement statement,
			List<Map<String, String>> expressionList) {
		int index = 1;
		try {
			for (Map<String,String> exp : expressionList) {
				setQueryStatementString(statement, index, exp.get(PROPERTY), exp.get(ARGUMENTS));
				index++;
			}
		} catch (SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.BUILDING_SQL_ERROR, e);
		}
	}

	/**
	 * Sets the values for fields.
	 *
	 * @param dataType  the data type
	 * @param dataValue the data value
	 * @param statement the statement
	 * @param index     the index
	 * @param key       the key
	 * @throws SQLException the SQL exception
	 */
	private void setValuesForFields(String dataType, String dataValue, PreparedStatement statement, int index,
			String key) throws SQLException {

		switch (dataType) {
			case (SnowflakeDataTypeConstants.SNOWFLAKE_NUMBERTYPE):
				setIntValue(dataValue, statement, index);
				break;
			case (SnowflakeDataTypeConstants.SNOWFLAKE_DOUBLETYPE):
				setFloatValue(dataValue, statement, index);
				break;
			case (SnowflakeDataTypeConstants.SNOWFLAKE_BOOLEANTYPE):
				setBooleanValues(dataValue, statement, index, key);
				break;
			case (SnowflakeDataTypeConstants.SNOWFLAKE_DATETYPE):
				setDateValue(dataValue, statement, index);
				break;
			case (SnowflakeDataTypeConstants.SNOWFLAKE_TIMETYPE):
				setTimeValue(dataValue, statement, index);
				break;
			case (SnowflakeDataTypeConstants.SNOWFLAKE_TIMESTAMP_NTZ):
			case (SnowflakeDataTypeConstants.SNOWFLAKE_TIMESTAMP_LTZ):
			case (SnowflakeDataTypeConstants.SNOWFLAKE_TIMESTAMP_TZ):
				setDateTimeValue(dataValue, statement, index);
				break;
			case (SnowflakeDataTypeConstants.SNOWFLAKE_BINARYTYPE):
			case (SnowflakeDataTypeConstants.SNOWFLAKE_VARCHARTYPE):
				setStringValue(dataValue, statement, index);
				break;
			default:
				setStringValue(dataValue, statement, index);
		}
	}

	/**
	 * Sets values form JSON Objects into a parameterized statement
	 *
	 * @param statement       parameterized statement to be filled
	 * @param dataSortedMap   the Map Object contains the values
	 * @param fitlerSortedMap the Map Object contains the filtering values
	 * @param emptyFieldSelection
	 * @param batchSize
	 */
	public void fillStatementValuesWithDataTypeForCreate(PreparedStatement statement,
			SortedMap<String, String> dataSortedMap, SortedMap<String, String> fitlerSortedMap,
			String emptyFieldSelection, SortedMap<String, String> metadata, Long batchSize) {
		if (emptyFieldSelection.equalsIgnoreCase(SnowflakeDataTypeConstants.DEFAULT_SELECTION) && batchSize == 1) {
			fillStatementValuesWithDataTypeForDefaultSelection(statement, dataSortedMap, fitlerSortedMap, metadata);
		} else {
			fillStatementValuesWithDataType(statement, dataSortedMap, fitlerSortedMap, metadata);
		}
	}

	/**
	 * Fill statement values with data type for default selection.
	 *
	 * @param statement the statement
	 * @param dataSortedMap the data sorted map
	 * @param fitlerSortedMap the fitler sorted map
	 */
	public void fillStatementValuesWithDataTypeForDefaultSelection(PreparedStatement statement,
			SortedMap<String, String> dataSortedMap, SortedMap<String, String> fitlerSortedMap,
			SortedMap<String, String> metadata) {
		validateInputAgainstObjectDefinition(dataSortedMap, dataObjectDefinition);
		validateInputAgainstObjectDefinition(fitlerSortedMap, filterObjectDefinition);
		int index = 1;
		try {
			for (String keyStr : dataSortedMap.keySet()) {
				index = setFieldValues(statement, dataSortedMap, metadata, index, keyStr);
			}
			for (String keyStr : filterObjectDefinition.keySet()) {
				index = setStatementString(statement, fitlerSortedMap, index, keyStr);
			}

		} catch (SQLException e) {
			throw new ConnectorException(SnowflakeDataTypeConstants.BUILDING_SQL_ERROR, e);
		}
	}
	private int setFieldValues(PreparedStatement statement, SortedMap<String, String> dataSortedMap,
			SortedMap<String, String> metadata, int index, String keyStr) throws SQLException {
		if (metadata != null && metadata.get(keyStr) != null) {
			setValuesForFields(metadata.get(keyStr), dataSortedMap.get(keyStr), statement, index, keyStr);
			index++;
		}
		return index;
	}

	private int setStatementString(PreparedStatement statement, SortedMap<String, String> fitlerSortedMap, int index,
			String keyStr) throws SQLException {
		if (fitlerSortedMap.get(keyStr) == null) {
			throw new ConnectorException(SnowflakeDataTypeConstants.MISSING_COLUMN_VALUE_ERROR + keyStr);
		} else {
			statement.setString(index, fitlerSortedMap.get(keyStr));
		}
		index++;
		return index;
	}

	private void setQueryStatementString(PreparedStatement statement, int index,
			String keyStr, String value) throws SQLException {
		Args.notNull(value,SnowflakeDataTypeConstants.MISSING_COLUMN_VALUE_ERROR + keyStr);
		statement.setString(index, value);
	}

	/**
	 * Sets the Boolean value
	 *
	 * @param value     value to be set
	 * @param statement statement to set
	 * @param index     index for the value
	 * @param key       name of the field
	 * @throws SQLException
	 */
	private void setBooleanValues(String value, PreparedStatement statement, int index, String key)
			throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.BOOLEAN);
		} else {
			try {
				float val = Float.parseFloat(value);
				if (Float.compare(val, 0.0f) == 0) {
					statement.setBoolean(index, Boolean.FALSE);
				} else {
					statement.setBoolean(index, Boolean.TRUE);
				}
			} catch (NumberFormatException e) {
				if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("f") || value.equalsIgnoreCase("no")
						|| value.equalsIgnoreCase("n") || value.equalsIgnoreCase("off")) {
					statement.setBoolean(index, Boolean.FALSE);
				} else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("t") || value.equalsIgnoreCase(
						"yes") || value.equalsIgnoreCase("y") || value.equalsIgnoreCase("on")) {
					statement.setBoolean(index, Boolean.TRUE);
				} else {
					throw new ConnectorException("Please enter valid Boolean input for the field: " + key);
				}
			}
		}
	}

	/**
	 * Sets the Float value
	 *
	 * @param value     value to be set
	 * @param statement statement to set
	 * @param index     index for the value
	 * @throws SQLException
	 */
	private void setIntValue(String value, PreparedStatement statement, int index) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.BIGINT);
		} else {
			BigDecimal bigint = new BigDecimal(value);
			statement.setBigDecimal(index, bigint);
		}
	}

	/**
	 * Sets the Float value
	 *
	 * @param value     value to be set
	 * @param statement statement to set
	 * @param index     index for the value
	 * @throws SQLException
	 */
	private void setFloatValue(String value, PreparedStatement statement, int index) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.DOUBLE);
		} else {
			if (value.equalsIgnoreCase("nan")) {
				statement.setDouble(index, Double.NaN);
			} else if (value.equalsIgnoreCase("inf")) {
				statement.setDouble(index, Double.POSITIVE_INFINITY);
			} else if (value.equalsIgnoreCase("-inf")) {
				statement.setDouble(index, Double.NEGATIVE_INFINITY);
			} else {
				statement.setDouble(index, Double.valueOf(value));
			}
		}
	}

	/**
	 * Sets the Date value
	 *
	 * @param value     value to be set
	 * @param statement statement to set
	 * @param index     index for the value
	 * @throws SQLException
	 */
	private void setDateValue(String value, PreparedStatement statement, int index) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.DATE);
		} else if (_connectionTimeFormat.getDateFormat().equalsIgnoreCase(AUTO_FORMAT)) {
			statement.setString(index, value);
		} else {
			try {
				java.util.Date date1 = new SimpleDateFormat(_connectionTimeFormat.getDateFormat()).parse(value);
				Date date = new Date(date1.getTime());
				statement.setDate(index, Date.valueOf(date.toString()));
			} catch (ParseException e) {
				throw new ConnectorException(e);
			}
		}
	}

	/**
	 * Sets the Time value
	 *
	 * @param value     value to be set
	 * @param statement statement to set
	 * @param index     index for the value
	 * @throws SQLException
	 */
	private void setTimeValue(String value, PreparedStatement statement, int index) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.TIME);
		} else if (_connectionTimeFormat.getTimeFormat().equalsIgnoreCase(AUTO_FORMAT)) {
			statement.setString(index, value);
		} else {
			try {
				java.util.Date date1 = new SimpleDateFormat(_connectionTimeFormat.getTimeFormat()).parse(value);
				Time time = new Time(date1.getTime());
				statement.setTime(index, Time.valueOf(time.toString()));
			} catch (ParseException e) {
				throw new ConnectorException(e);
			}
		}
	}

	/**
	 * Sets the DateTime value
	 *
	 * @param value     value to be set
	 * @param statement statement to set
	 * @param index     index for the value
	 * @throws SQLException
	 */
	private void setDateTimeValue(String value, PreparedStatement statement, int index) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.TIMESTAMP);
		} else if (_connectionTimeFormat.getDateTimeFormat().equalsIgnoreCase(AUTO_FORMAT)) {
			statement.setString(index, value);
		} else {
			DateTimeFormatter formatter = DateTimeFormat.forPattern(_connectionTimeFormat.getDateTimeFormat());
			DateTime dateTime = formatter.parseDateTime(value);
			Timestamp stamp = new Timestamp(dateTime.getMillis());
			statement.setTimestamp(index, stamp);
		}
	}

	/**
	 * Sets the String value
	 *
	 * @param value     value to be set
	 * @param statement statement to set
	 * @param index     index for the value
	 * @throws SQLException
	 */
	private void setStringValue(String value, PreparedStatement statement, int index) throws SQLException {
		if (value == null) {
			statement.setNull(index, Types.VARCHAR);
		} else {
			statement.setString(index, value);
		}
	}
}
