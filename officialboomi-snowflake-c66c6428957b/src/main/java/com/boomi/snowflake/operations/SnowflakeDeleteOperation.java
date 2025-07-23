// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.operations;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.util.BaseDeleteOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.override.ConnectionOverrideUtil;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;

/**
 * The SnowflakeDeleteOperation class
 * @author s.vanangudi
 *
 */
public class SnowflakeDeleteOperation extends BaseDeleteOperation {
	
	/** The Constant APPLICATION_ERROR_MESSAGE. */
	private static final String APPLICATION_ERROR_MESSAGE = "Error in batch %d: ";

	/**
	 * Instantiates a SnowflakeDeleteOperation.
	 * @param conn
	 * 			the Snowflake Connection
	 * */
	public SnowflakeDeleteOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * this function is called when DELETE operation is done
	 * @param request Delete Request
	 * @param response Operation Response
	 */
	@Override
	protected void executeDelete(DeleteRequest request, OperationResponse response) {
		response.getLogger().entering(this.getClass().getCanonicalName(), "executeDelete()");
		
		ConnectionProperties properties = null;
		SnowflakeWrapper wrapper = null;
		try {
			PropertyMap operationProperties= getContext().getOperationProperties();
			properties = new ConnectionProperties(getConnection(),
					operationProperties, getContext().getObjectTypeId(),
					response.getLogger());
			Long batchSize = properties.getBatchSize();
			SortedMap<String, String> filterJSONObj = properties.getFilterObject();
			wrapper = SnowflakeOperationUtil.setupWrapper(properties);
			String tableName = SnowflakeOperationUtil.getTableName(properties);
			SortedMap<String, String> metadata = null;
			if (tableName != null) {
				metadata = SnowflakeOperationUtil.getTableMetadata(tableName, properties.getConnectionGetter()
						.getConnection(properties.getLogger()),
								operationProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
								operationProperties.getProperty(SnowflakeOverrideConstants.SCHEMA));
			}
			handleDelete(request, response, wrapper, batchSize, filterJSONObj, metadata);

			if (batchSize > 1 && wrapper.getBatchedCount() != 0) {
				wrapper.executeQuery(batchSize);
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		} finally {
			/*
			 * closes wrapper if it's not null
			 * otherwise commit and close properties
			 */
			if (wrapper == null) {
				if (properties != null) {
					properties.commitAndClose();
				}
			} else {
				wrapper.close();
			}
		}
	}

	private void handleDelete(DeleteRequest request, OperationResponse response, SnowflakeWrapper wrapper,
			Long batchSize, SortedMap<String, String> filterJSONObj, SortedMap<String, String> metadata) {
		for (ObjectIdData input : request) {
			input.getLogger().info("Started processing");
			try {
				// parse JSON filter object
				SortedMap<String, String> data = JSONHandler.readSortedMap(input.getObjectId());
				for (String keyStr : filterJSONObj.keySet()) {
					filterJSONObj.put(keyStr, data.get(keyStr));
					data.remove(keyStr);
				}

				// construct and fill a statement
				if (batchSize == 1 || wrapper.getPreparedStatement() == null) {
					wrapper.setPreparedStatement(wrapper.constructDeleteStatement(filterJSONObj));
				}
				ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(
						wrapper.getPreparedStatement().getConnection(), input.getDynamicOperationProperties());
				wrapper.fillStatementValuesWithDataType(wrapper.getPreparedStatement(), new TreeMap<>(), filterJSONObj,
						metadata);

				wrapper.executeHandler(batchSize);
				// return the filter object
				ResponseUtil.addSuccess(response, input, "0", null);
			} catch (ConnectorException e) {
				String errorMessage = String.format(APPLICATION_ERROR_MESSAGE, wrapper.getCurrentBatch())
						+ e.getMessage();
				input.getLogger().log(Level.WARNING, errorMessage, e);
				response.addResult(input, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), errorMessage,
						ResponseUtil.toPayload(errorMessage));
			} catch (Exception e) {
				input.getLogger().log(Level.SEVERE, e.getMessage());
				ResponseUtil.addExceptionFailure(response, input, e);
			}
		}
	}

	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}