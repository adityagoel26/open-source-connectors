// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Deque;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.commands.CommandFactory;
import com.boomi.snowflake.commands.ISnowflakeCommand;
import com.boomi.snowflake.override.ConnectionOverrideUtil;

/**
 * The Class ConnectionProperties.
 *
 * @author Vanangudi,S
 */
public class ConnectionProperties {
	
	/** The Constant DEFAULT_STAGE_PATH. */
	private static final String DEFAULT_STAGE_PATH = "boomi/$OPERATION/$DATE/$TIME/$UUID/";
	/** The Constant PROP_STAGE_TMP_PATH. */
	private static final String PROP_STAGE_TMP_PATH = "stageTempPath";
	/** The Constant PROP_NUMBER_OF_SCRIPTS. */
	private static final String PROP_NUMBER_OF_SCRIPTS = "numberOfScripts";
	/** The Connection Object. */
	private Connection _connection;
	/** The Operation details. */
	private String _tableName, _bucketName, _region, _secret, _accessKey, _stageName, _filePath;
	/** The Batch Size. */
	private Long _batchSize;
	/** The Truncate option. */
	private Boolean _truncate;
	/** The Header Option. */
	private Boolean _header;
	/** The ConnectionTimeFormat object. */
	private ConnectionTimeFormat _connectionTimeFormat;
	/** The Logger object. */
	private Logger _logger;
	/** The Column Names. */
	private String _columnNames;
	/** The Parallel Upload. */
	private Long _parallelUpload;
	/** The Auto Compress Option. */
	private Boolean _autoCompress;
	/** The Compression Name. */
	private String _Compression;
	/** The Souce Compression. */
	private String _sourceCompression;
	/** The Overwrite Option. */
	private Boolean _overwrite;
	/** The File format type. */
	private String _fileFormatType;
	/** The Other Format Names. */
	private String _otherFormatOptions;
	/** The Copy Options. */
	private String _copyOptions;
	/** The File format Name. */
	private String _fileFormatName;
	/** The Batching Option. */
	private Boolean _applyBatching;
	/** The SnowflakeConnection object. */
	private SnowflakeConnection _snfConnection;
	/** The Statement object. */
	private Statement _statement;
	/** The Chunk Size. */
	private Long _chunkSize;
	/** The Return Results option. */
	private boolean _returnResults;
	/** The ISnowflakeCommand object. */
	private ISnowflakeCommand _command;
	/** The Operation Type. */
	private String operationType;
	/** The Temporary Stage Path. */
	private String tmpStagePath;
	/** The Total Number of Scripts. */
	private Long _numberOfScripts;
	/** The Document Batching Option. */
	private boolean _documentBatching;
	/** The empty value input. */
	private String emptyValueInput;
	/** The Constant NUMBER_OF_SCRIPT. */
	private static final String NUMBER_OF_SCRIPTS_NON_ZERO = "numberOfScriptsNonZero";
	/** The Constant EMPTY_VALUE_INPUT. */
	private static final String INPUT_OPTIONS = "inputOptionsForMissingFields";
	private Long numberOfNonZeroScripts;
	private static final String AWS_BUCKET_NAME = "awsBucketName";
	private static final String AWS_REGION = "awsRegion";
	private static final String STAGE_NAME = "stageName";
	private static final String FILE_PATH = "filePath";
	private static final String DATABASE_ACCESS_ERROR = "Database access error";
	private final boolean _isOverrideEnabled;

	/** 
	 * Instantiates a new connection properties
	 * @param connection connection object of the connector
	 * @param operationProperties operation properties of the connection
	 * @param tableName table Name for the operation
	 * @param processLogger Logger object
	 */
	@SuppressWarnings("deprecation")	// Sonar issue: java:S3010
	public ConnectionProperties(SnowflakeConnection connection, PropertyMap operationProperties, String tableName,
			Logger processLogger) {
		if (connection.getOperationContext().getCustomOperationType() == null) {
			operationType = connection.getOperationContext().getOperationType().toString();
		} else {
			operationType = connection.getOperationContext().getCustomOperationType();
		}
		tmpStagePath = operationProperties.getOrDefault(PROP_STAGE_TMP_PATH, DEFAULT_STAGE_PATH).toString();
		_command = CommandFactory.getCommand(connection, operationProperties, tableName);
		_statement = null;
		_connection = null;
		_snfConnection = connection;
		_connectionTimeFormat = _snfConnection.getConnectionTimeFormat();
		_tableName = tableName;
		_batchSize = validatePositivity("Batch Size",operationProperties.getLongProperty("batchSize", (long) 1));
		_parallelUpload = validatePositivity("Paralleism", operationProperties.getLongProperty("parallelUpload", (long) 4));
		_bucketName = operationProperties.getProperty(AWS_BUCKET_NAME,"");
		_region = operationProperties.getProperty(AWS_REGION,"");
		_Compression = operationProperties.getProperty("compression","");
		_sourceCompression = operationProperties.getProperty("sourceCompression","");
		_secret = _snfConnection.getAWSSecret();
		_accessKey = _snfConnection.getAWSAccessKey();
		_truncate = operationProperties.getBooleanProperty("truncate",false);
		_header = operationProperties.getBooleanProperty("header",false);
		_autoCompress = operationProperties.getBooleanProperty("autoCompress",false);
		_overwrite = operationProperties.getBooleanProperty("overwrite",false);
		_stageName = operationProperties.getProperty(STAGE_NAME,"");
		_filePath = operationProperties.getProperty(FILE_PATH,"");
		_columnNames = operationProperties.getProperty("columns","");
		_fileFormatType = operationProperties.getProperty("fileFormatType","");
		_otherFormatOptions = operationProperties.getProperty("otherFormatOptions","");
		_copyOptions = operationProperties.getProperty("copyOptions","");
		_fileFormatName = operationProperties.getProperty("fileFormatName","");
		_applyBatching = operationProperties.getBooleanProperty("applyBatching", false);
		_logger = processLogger;
		_chunkSize = operationProperties.getLongProperty("chunkSize", (long) 250);
		_returnResults = operationProperties.getBooleanProperty("returnResults", false);
		_numberOfScripts = operationProperties.getLongProperty(PROP_NUMBER_OF_SCRIPTS, (long) 0);
		_documentBatching = operationProperties.getBooleanProperty("documentBatching",false);
		if (operationProperties.getLongProperty(NUMBER_OF_SCRIPTS_NON_ZERO) != null) {
			numberOfNonZeroScripts = operationProperties.getLongProperty(NUMBER_OF_SCRIPTS_NON_ZERO);
			_numberOfScripts = numberOfNonZeroScripts;
		}
		emptyValueInput = operationProperties.getProperty(INPUT_OPTIONS, "");
		//checks if connection override is enabled at runtime.
		_isOverrideEnabled = ConnectionOverrideUtil.isOverrideEnabled(operationProperties);
	}
	
	/**
	 * Gets the empty value input.
	 *
	 * @return the empty value input
	 */
	public String getEmptyValueInput() {
		return emptyValueInput;
	}

	/**
	 * Replace the variables to the Snowflake supported String
	 *
	 * @param rawString the String with variable name
	 * @return the formatted String
	 */
	private String replaceVariablesInString(String rawString) {
		return rawString.replace("$TIME", DateTimeFormatter.ofPattern("HHmmss.SSS").format(LocalDateTime.now()))
				.replace("$DATE", DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now()))
				.replace("$OPERATION", operationType)
				.replace("$UUID", UUID.randomUUID().toString());
	}
	
	/**
	 * Make sure that the String ends with the back slash
	 *
	 * @param rawString the input String
	 * @return String that ends with back slash
	 */
	private String assureStringEndsWithBackSlash(String rawString) {
		return rawString + ((rawString.length() != 0 && rawString.charAt(rawString.length() - 1) == '/') ? "" : "/");
	}
	
	/**
	 * Gets the Temporary Stage path.
	 *
	 * @param inputDocument the input document object
	 * @return the Temporary Stage path String
	 */
	public String getStageTempPath(ObjectData inputDocument) {
		String dpStagePath = inputDocument.getDynamicProperties().getOrDefault(PROP_STAGE_TMP_PATH, tmpStagePath);
		if(dpStagePath.trim().length() == 0) {
			dpStagePath = DEFAULT_STAGE_PATH;
		}
		return assureStringEndsWithBackSlash(replaceVariablesInString(dpStagePath));
	}
	
	/**
	 * Gets the Return Results options.
	 *
	 * @return the result options
	 */
	public boolean getReturnResults() {
		return _returnResults;
	}
	
	/**
	 * call this function to separate column names
	 * 
	 * @return SortedMap object containing the column names as a key
	 */
	public SortedMap<String, String> getFilterObject() {
		String[] conditionColumns = _columnNames.trim().split(",");
		SortedMap<String, String> filter = new TreeMap<String, String>();
		try {
			// parse and trim column names
			if (conditionColumns == null || conditionColumns.length == 0) {
				throw new IllegalArgumentException();
			}
			
			for (int i = 0; i < conditionColumns.length; i++) {
				conditionColumns[i] = conditionColumns[i].trim();
				if (conditionColumns[i] == null || conditionColumns[i].length() == 0) {
					throw new IllegalArgumentException();
					
				}
			}
		} catch (IllegalArgumentException e) {
			throw new ConnectorException("Error parsing filter column names. Column name(s) should be seperated by comma", e);
		}
		for (int i = 0; i < conditionColumns.length; i++) {
			filter.put(conditionColumns[i], "");
		}
		return filter;
	}
	
	/**
	 * Validate the positivity of the property.
	 *
	 * @param propertyName property name to be validated
	 * @param param property value
	 * @return the param value if positive or else throws the exception
	 */
	private Long validatePositivity(String propertyName,Long param) {
		if (param < 1) {
			throw new ConnectorException(propertyName + " must be a positive integer");
		}
		
		return param;
	}
	
	/**
	 * Gets the Column Names.
	 *
	 * @return the Column Names String
	 */
	public String getColumnNames() {
		return _columnNames;
	}
	
	/**
	 * Gets the Format Options.
	 *
	 * @return the format options String
	 */
	public String getOtherFormatOptions() {
		return _otherFormatOptions;
	}
	
	/**
	 * Gets the File Format Type.
	 *
	 * @return the file format type String
	 */
	public String getFileFormatType() {
		return _fileFormatType;
	}
	
	/**
	 * Gets the Compression.
	 *
	 * @return the Compression String
	 */
	public String getCompression() {
		return _Compression;
	}
	
	/**
	 * Gets the Source Compression.
	 *
	 * @return the source Compression String
	 */
	public String getSourceCompression() {
		return _sourceCompression;
	}
	
	/**
	 * Gets the Parallelism.
	 *
	 * @return the Parallelism Long value
	 */
	public Long getParallelism() {
		return _parallelUpload;
	}
	
	/**
	 * Gets the Auto Compress option.
	 *
	 * @return the Auto Compress option
	 */
	public Boolean getAutoCompress() {
		return _autoCompress;
	}
	
	/**
	 * Gets the Overwrite option.
	 *
	 * @return the Overwrite option
	 */
	public Boolean getOverwrite() {
		return _overwrite;
	}
	
	/**
	 * Gets the Connection Getter object.
	 *
	 * @return the Connection Getter object
	 */
	public ConnectionGetter getConnectionGetter() {
		return new ConnectionGetter();
	}
	
	/**
	 * The Class ConnectionGetter.
	 */
	public class ConnectionGetter {
		
		public ConnectionGetter() {
			// no-arg constructor
		}
		
		/** 
		 * Gets the Connection Object
		 * 
		 * @param processLogger Logger Object
		 * @return Connection Object
		 */
		public Connection getConnection(Logger processLogger) {
			if(_connection == null) {
				_connection = _snfConnection.createJdbcConnection();
				try {
					if(processLogger != null) {
						if (_connection.getCatalog() == null) {
							processLogger.warning("Invalid Database name");
						}
						
						if (_connection.getSchema() == null) {
							processLogger.warning("Invalid Schema name");
						}
					}
				} catch (SQLException e) {
					throw new ConnectorException(DATABASE_ACCESS_ERROR, e);
				}
			}
			return _connection;
		}

		/**
		 * Retrieves a JDBC {@link Connection} object, applying any specified dynamic property overrides.
		 * <p>
		 * This method initially establishes the base JDBC connection via {@link #getConnection(Logger)}. It then
		 * modifies
		 * the connection's properties—such as catalog and schema—according to the dynamic properties provided.
		 * Should any issues arise during the connection retrieval or the application of these overrides, a
		 * {@link ConnectorException} will be thrown.
		 *
		 * @param processLogger     the {@link Logger} employed to capture detailed logs throughout the connection
		 *                                retrieval process
		 * @param dynamicProperties a map of dynamic properties used to adjust connection settings (e.g., catalog,
		 *                                schema)
		 * @return a {@link Connection} object with the applied dynamic overrides
		 * @throws ConnectorException if an error occurs during the connection retrieval or the application of dynamic
		 * properties
		 */
		public Connection getConnection(Logger processLogger,DynamicPropertyMap dynamicProperties) {
            try {
				getConnection(processLogger);
				ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(_connection,dynamicProperties);
                return _connection;
            } catch (SQLException e) {
				throw new ConnectorException(DATABASE_ACCESS_ERROR, e);
            }
        }
		/**
		 * Gets the Statement object.
		 *
		 * @return the statement object.
		 */
		public Statement getStatement() {
			if(_statement == null) {
				try {
					_statement = getConnection(null).createStatement();
				} catch (SQLException e) {
					throw new ConnectorException(DATABASE_ACCESS_ERROR, e);
				}
			}
			return _statement;
			
		}

		/**
		 * Retrieves a {@link Statement} for executing SQL commands, ensuring it is initialized if not already present.
		 * This method creates a new {@link Statement} object from the connection if it hasn't been created yet.
		 * @param dynamicPropertyMap The dynamic properties containing overrides for the database and schema.
		 * @return The {@link Statement} object associated with the current connection.
		 * @throws SQLException If a database access error occurs.
		 * @throws ConnectorException If there is a problem creating the {@link Statement}.
		 */
		public Statement getStatement(DynamicPropertyMap dynamicPropertyMap) throws SQLException, ConnectorException {

			getStatement();
			ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(_statement.getConnection(),
					dynamicPropertyMap);
			return _statement;
		}
		/**
		 * Closes the opened statement, connection
		 */
		public void close() {
			Deque<RuntimeException> exceptionStack = new LinkedList<>();
			boolean connectionPooling = _snfConnection.getContext().getConnectionProperties().getBooleanProperty(
					SnowflakeOverrideConstants.PROP_ENABLE_POOLING, false);
			// Reset connection  exclusive for override and pooling enabled.
			if (_isOverrideEnabled && connectionPooling) {
				ConnectionOverrideUtil.resetConnection(_snfConnection,_connection,exceptionStack);
			}
			try {
				if (_statement != null) {
					_statement.close();
				}
			} catch (SQLException e) {
				exceptionStack.push(new ConnectorException("Unable to close statement ", e));
            }
            try {
				if (_connection != null) {
					_connection.commit();
				}
			} catch (SQLException e) {
				exceptionStack.push(new ConnectorException("Unable to commit ", e));
			}
			try {
				if (_connection != null) {
					_connection.close();
				}
			} catch (SQLException e) {
				exceptionStack.push(new ConnectorException("Unable to close Snowflake connection ", e));
            }

            if (!exceptionStack.isEmpty()) {
				throw exceptionStack.pop();
			}
		}
	   }

	/**
	 * Gets the Table Name.
	 *
	 * @return the table name String
	 */
	public String getTableName() {
		return _tableName;
	}

	/**
	 * Gets the Bucket Name.
	 *
	 * @return the bucket name string.
	 */
	public String getBucketName() {
		return _bucketName;
	}

	/**
	 * Gets the AWS Region.
	 *
	 * @return the aws region string.
	 */
	public String getAWSRegion() {
		return _region;
	}

	/**
	 * Gets the Secret Key.
	 *
	 * @return the secret key string.
	 */
	public String getSecret() {
		return _secret;
	}

	/**
	 * Gets the Access Key.
	 *
	 * @return the access key string.
	 */
	public String getAccessKey() {
		return _accessKey;
	}

	/**
	 * Gets the Stage Name.
	 *
	 * @return the stage name string.
	 */
	public String getStageName() {
		return _stageName;
	}

	/**
	 * Gets the File Path.
	 *
	 * @return the file path string.
	 */
	public String getFilePath() {
		return _filePath;
	}

	/**
	 * Gets the Batch Size.
	 *
	 * @return the batch size value.
	 */
	public Long getBatchSize() {
		return _batchSize;
	}

	/**
	 * Gets the Truncate option.
	 *
	 * @return the truncate option.
	 */
	public Boolean getTruncate() {
		return _truncate;
	}
	
	/**
	 * Gets the Header Option.
	 *
	 * @return the Header Option.
	 */
	public Boolean getHeader() {
		return _header;
	}
	
	/**
	 * Gets the Copy Options.
	 *
	 * @return the copy options string.
	 */
	public String getCopyOptions() {
		return _copyOptions;
	}
	
	/**
	 * Gets the Batching option.
	 *
	 * @return the batching option.
	 */
	public Boolean ApplyBacthing() {
		return _applyBatching;
	}
	
	/**
	 * Gets the Connection Time Format.
	 *
	 * @return the Connection Time Format object.
	 */
	public ConnectionTimeFormat getConnectionTimeFormat() {
		return _connectionTimeFormat;
	}

	/**
	 * Gets the Logger object.
	 *
	 * @return the Logger object.
	 */
	public Logger getLogger() {
		return _logger;
	}
	
	/**
	 * Gets the File Format Name.
	 *
	 * @return the File Format Name String.
	 */
	public String getFileFormatName() {
		return _fileFormatName;
	}
	
	/**
	 * Gets the Number of Scripts.
	 *
	 * @return the Number of Scripts value.
	 */
	public Long getNumberOfScripts() {
		return _numberOfScripts;
	}
	
	/**
	 * Gets the number of non zero scripts.
	 *
	 * @return the number of non zero scripts
	 */
	public Long getNumberOfNonZeroScripts() {
		return numberOfNonZeroScripts;
	}
	/**
	 * Gets the Document Batching option.
	 *
	 * @return the Document Batching option.
	 */
	public Boolean getDocumentBatching() {
		return _documentBatching;
	}
	
	/**
	 * Gets the Chunk Size.
	 *
	 * @return the Chunk Size value.
	 */
	public long getChunkSize() {
		if(_chunkSize <= 0) {
			if(_chunkSize == -1) {
				return Integer.MAX_VALUE;
			}
			throw new ConnectorException("Invalid chunk size value");
		}
		return _chunkSize;
	}
	
	/**
	 * Gets the ISnowflakeCommand object.
	 *
	 * @return the ISnowflakeCommand object.
	 */
	public ISnowflakeCommand getSnowflakeCommand() {
		return _command;
	}
	
	/**
	 * Commit and close the opened statements and connections
	 */
	public void commitAndClose() {
		Deque<RuntimeException> exceptionStack = new LinkedList<>();
		if (_connection == null) {
			return;
		}
		boolean connectionPooling = _snfConnection.getContext().getConnectionProperties().getBooleanProperty(
				SnowflakeOverrideConstants.PROP_ENABLE_POOLING, false);
		if (_isOverrideEnabled && connectionPooling) {
			ConnectionOverrideUtil.resetConnection(_snfConnection,_connection,exceptionStack);
		}
		try {
			if (_statement != null) {
				_statement.close();
			}
		} catch (SQLException e) {
			exceptionStack.push(new ConnectorException("Unable to close connection", e));
		}
		try {
			_connection.commit();
		} catch (SQLException e) {
			exceptionStack.push(new ConnectorException("Unable to commit", e));
		}
		try {
			_connection.close();
		} catch (SQLException e) {
			exceptionStack.push(new ConnectorException("Unable to close connection", e));
		}

		if (!exceptionStack.isEmpty()) {
			throw exceptionStack.pop();
		}
	}

	/**
	 * Sets the value for S3 Bucket Name from Dynamic Operation Properties
	 * If the value is null then it defaults to Operation Properties value
	 *
	 * @param dynamicProperties the input document dynamic operation properties
	 */
	public void setDynamicBucketName(DynamicPropertyMap dynamicProperties) {
		_bucketName = dynamicProperties.getProperty(AWS_BUCKET_NAME);
	}

	/**
	 * Sets the value for AWS region from Dynamic Operation Properties
	 * If the value is null then it defaults to Operation Properties value
	 *
	 * @param dynamicProperties the input document dynamic operation properties
	 */
	public void setDynamicAWSRegion(DynamicPropertyMap dynamicProperties) {
		_region = dynamicProperties.getProperty(AWS_REGION);
	}

	/**
	 * Sets the value for Internal Stage Name from Dynamic Operation Properties
	 * If the value is null then it defaults to Operation Properties value
	 *
	 * @param dynamicProperties the input document dynamic operation properties
	 */
	public void setDynamicStageName(DynamicPropertyMap dynamicProperties) {
		_stageName = dynamicProperties.getProperty(STAGE_NAME);
	}

	/**
	 * Sets the value for Internal Source Files Path from Dynamic Operation Properties
	 * If the value is null then it defaults to Operation Properties value
	 *
	 * @param dynamicProperties the input document dynamic operation properties
	 */
	public void setDynamicFilePath(DynamicPropertyMap dynamicProperties) {
		_filePath =  dynamicProperties.getProperty(FILE_PATH);
	}

	/**
	 * Sets the value for Dynamic Operation Properties
	 *
	 * @param dynamicProperties the input document dynamic operation properties
	 */
	public void setDynamicProperties(DynamicPropertyMap dynamicProperties) {
		setDynamicBucketName(dynamicProperties);
		setDynamicAWSRegion(dynamicProperties);
		setDynamicStageName(dynamicProperties);
		setDynamicFilePath(dynamicProperties);
	}
}
