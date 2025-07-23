// Copyright (c) 2025 Boomi, LP
package com.boomi.snowflake.controllers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.SortedMap;
import java.util.TreeMap;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.snowflake.override.ConnectionOverrideUtil;
import com.boomi.snowflake.stages.AmazonWebServicesHandler;
import com.boomi.snowflake.stages.SnowflakeInternalStageHandler;
import com.boomi.snowflake.util.BoundedMap;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.TableDefaultAndMetaDataObject;
import com.boomi.snowflake.util.SnowflakeDataTypeConstants;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.wrappers.BulkLoadFiles;
import com.boomi.snowflake.wrappers.BulkLoadWrapper;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.boomi.util.StringUtil;

import net.snowflake.client.jdbc.internal.net.minidev.json.JSONObject;

/**
 * The Class SnowflakeCreateController.
 *
 * @author Vanangudi,S
 */
public class SnowflakeCreateController {

	/** The Batch Size. */
	private Long _batchSize;
	/** The Snowflake Wrapper object. */
	private SnowflakeWrapper _wrapper;
	/** The Bulk Load Wrapper object. */
	private BulkLoadWrapper _bulkLoadWrapper;
	/** The Bulk Load Files object. */
	private BulkLoadFiles _bulkloadFiles;
	/** The Result Set object. */
	private ResultSet _resultSet;
	/** The Update Count. */
	private Integer _updateCount;
	/** The Batched Count. */
	private int _batchedCount;
	/** The Constant SNOWFLAKE_UPDATE_COUNT. */
	private static final String SNOWFLAKE_UPDATE_COUNT = "Update_Count";
	private final boolean _truncate;

	/**
	 * Instantiates a new Snowflake Create Controller
	 * @param properties connection properties set for this operation
	 */
	public SnowflakeCreateController(ConnectionProperties properties) {
		setConnectionProperties(properties);
		_truncate = properties.getTruncate();
	}

	private void setConnectionProperties(ConnectionProperties properties){
		if (StringUtil.isEmpty(properties.getBucketName()) && StringUtil.isEmpty(properties.getStageName())) {
			properties.getLogger().fine(
					"Loading Database Connection and Operation settings and establishing connection.");
			_batchSize = properties.getBatchSize();
			if (_batchSize == null || _batchSize < 1) {
				throw new ConnectorException("Batch Size must be a positive integer");
			}

			_wrapper = new SnowflakeWrapper(properties.getConnectionGetter(),
					properties.getConnectionTimeFormat(), properties.getLogger(), properties.getTableName());

			if (_batchSize > 1) {
				_wrapper.setAutoCommit(false);
			}

			_wrapper.setPreparedStatement(null);
		}
	}

	/**
	 * Gets the Current Batch
	 * @return the current batch number
	 */
	public int getCurrentBatch() {
		return _wrapper.getCurrentBatch();
	}

	/**
	 * Processes the input data and manages database interactions, including constructing and executing
	 * prepared statements, applying default values, and handling dynamic properties.
	 *
	 * <p>This method ensures the prepared statement is properly constructed or reused, dynamically adjusts
	 * the connection properties, and fills statement values with appropriate data types for the operation.
	 * It also truncates the table if required and executes the batch operation.</p>
	 *
	 * @param input               A sorted map of input data to be processed and included in the database operation.
	 * @param emptyFieldSelection A flag determining the behavior for handling empty fields.
	 * @param metadata            A sorted map containing metadata associated with the operation.
	 * @param boundedMap          A bounded map acting as a cache for default values based on the database and schema.
	 * @param tableName          The connection properties, including table details and logger configuration.
	 * @param dynamicProperties   A dynamic property map influencing connection and operation behavior.
	 * @throws SQLException       If a database access error occurs.
	 * @throws ConnectorException If an error occurs while overriding the connection with dynamic properties.
	 */
	public void receive(SortedMap<String, String> input, String emptyFieldSelection,
				SortedMap<String, String> metadata, BoundedMap<String, TableDefaultAndMetaDataObject> boundedMap,
				String tableName, DynamicPropertyMap dynamicProperties) throws SQLException {
		// construct and fill a statement
		if (_wrapper.getPreparedStatement() == null ||
				(SnowflakeDataTypeConstants.DEFAULT_SELECTION.equals(emptyFieldSelection) && _batchSize == 1)) {
			_wrapper.setPreparedStatement(_wrapper.constructInsertStatement(input, emptyFieldSelection, metadata,
					_batchSize,dynamicProperties));
		} else {
			try {
				ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(
						_wrapper.getPreparedStatement().getConnection(), dynamicProperties);
			} catch (SQLException e) {
				throw new ConnectorException("Database access error", e);
			}
		}
		if(_batchSize >1){
			SnowflakeOperationUtil.setDefaultValuesForDBAndSchema(_wrapper,input,
					emptyFieldSelection, tableName, boundedMap, dynamicProperties, _batchSize);
		}
		_wrapper.fillStatementValuesWithDataTypeForCreate(_wrapper.getPreparedStatement(),
				input, new TreeMap<>(), emptyFieldSelection, metadata, _batchSize);
		if(_truncate){
			_wrapper.truncateTableIfNotDone(dynamicProperties);
		}
		_wrapper.executeHandler(_batchSize);
	}
	
	/**
	 * Sends file to Wrapper
	 *
	 * @param inputFile  stream containing sent file
	 * @param properties the connection properties
	 * @param inputDocument the input document
	 */
	public void receive(InputStream inputFile, ObjectData inputDocument, ConnectionProperties properties) {
		if (StringUtil.isNotEmpty(properties.getBucketName())) {
			AmazonWebServicesHandler awsHandler = getAWSHandler(properties);
			if (_bulkLoadWrapper != null) {
				_bulkLoadWrapper.setStageHandler(awsHandler);
			} else {
				_bulkLoadWrapper = new BulkLoadWrapper(properties , awsHandler);
			}
			_bulkLoadWrapper.uploadData(inputFile, inputDocument, properties);
		} else if (StringUtil.isNotEmpty(properties.getStageName())) {
			SnowflakeInternalStageHandler internalStage = getInternalStageHandler(properties);
			if (_bulkloadFiles != null) {
				_bulkloadFiles.setFilePath(properties.getFilePath());
				_bulkloadFiles.setStageHandler(internalStage);
			} else {
				_bulkloadFiles = new BulkLoadFiles(properties, internalStage);
			}
			_bulkloadFiles.start(inputDocument, properties);
		}
	}
	
	/**
	 * Finalize the last steps of create
	 */
	public void executeLastBatch() {
			try {
				// commit uncommitted batches
				if (_batchSize > 1 && _wrapper.getBatchedCount() != 0) {
					_wrapper.executeQuery(_batchSize);
				}
			}catch(Exception e) {
				throw e;
			}
	}

	/**
	 * closes the connections
	 */
	public void closeResources() {
		if (_bulkloadFiles != null) {
			_bulkloadFiles.close();
		} else if (_bulkLoadWrapper != null) {
			_bulkLoadWrapper.close();	
		} else if (_wrapper != null) {
			_wrapper.close();
		}
	}
	
	/**
	 * constructs output from a single statement from the SQL script
	 * @param isLastData boolean to determine if the result set is the last one or not
	 * @return input stream containing array of JSON elements
	 */
	public InputStream getResultFromStatement(boolean isLastData) {
		try {
			if(_batchSize == 1) {
				if(_resultSet == null && _updateCount == null) {
					_resultSet = _wrapper.getPreparedStatement().getResultSet();
					if(_resultSet.next()) {
						_updateCount = Integer.parseInt(_resultSet.getString(1));
					}
				}
			}else {
				if(++_batchedCount == _batchSize && !isLastData) {
					_batchedCount = 0;
					_updateCount = _wrapper.getPreparedStatement().getUpdateCount();
				}else if(isLastData) {
					_updateCount = _wrapper.getPreparedStatement().getUpdateCount();
				}else{
					_updateCount = -1;
				}
			}
		}catch(SQLException e) {
			throw new ConnectorException("Unable to retrieve data from Multi - Statement: ", e);
		}
		
		if(_updateCount != -1) {
			JSONObject jsonObj = new JSONObject();
			jsonObj.put(SNOWFLAKE_UPDATE_COUNT, _updateCount);
			String jsonStr = "["+ jsonObj.toString() +"]";
			return new ByteArrayInputStream(jsonStr.getBytes(StandardCharsets.UTF_8));
		}
		
		return null;
	}

	private static SnowflakeInternalStageHandler getInternalStageHandler(ConnectionProperties properties) {
		properties.getLogger().fine("Preparing to connect to Snowflake internal stage.");
		return new SnowflakeInternalStageHandler(
				properties.getConnectionGetter(),
				properties.getStageName(),
				properties.getParallelism(),
				properties.getAutoCompress(),
				properties.getSourceCompression(),
				properties.getOverwrite());
	}

	private static AmazonWebServicesHandler getAWSHandler(ConnectionProperties properties) {
		if(properties.getAWSRegion().isEmpty()) {
			throw new ConnectorException("Select a valid AWS Region");
		}
		properties.getLogger().fine(
				"Loading Amazon Web Services Connection and Operation settings and establishing connection.");
		return new AmazonWebServicesHandler(
				properties.getBucketName(), properties.getAccessKey(),
				properties.getSecret(), properties.getAWSRegion());
	}
}
