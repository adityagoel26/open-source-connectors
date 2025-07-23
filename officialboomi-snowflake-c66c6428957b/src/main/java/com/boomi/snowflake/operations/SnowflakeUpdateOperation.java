// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.operations;

import java.io.InputStream;
import java.util.SortedMap;
import java.util.logging.Level;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.override.ConnectionOverrideUtil;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.boomi.util.IOUtil;

/**
 * The SnowflakeUpdateOperation class
 * @author s.vanangudi
 *
 */
public class SnowflakeUpdateOperation extends SizeLimitedUpdateOperation {

	/** The Constant APPLICATION_ERROR_MESSAGE. */
	private static final String APPLICATION_ERROR_MESSAGE = "Error in batch %d: ";

	/**
	 * Instantiates a SnowflakeUpdateOperation.
	 * @param conn
	 * 			the Snowflake Connection
	 * */
	@SuppressWarnings("unchecked")
	public SnowflakeUpdateOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * This function is called when UPDATE operation is called
	 * @param request Update Request
	 * @param response Operation Response
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		response.getLogger().entering(this.getClass().getCanonicalName(), "executeUpdate()");
		SnowflakeWrapper wrapper = null;
		ConnectionProperties properties = null;
		
		try {
			PropertyMap operationProperties = getContext().getOperationProperties();
			properties = new ConnectionProperties(getConnection(),
					getContext().getOperationProperties(), getContext().getObjectTypeId(),
					response.getLogger());
			Long batchSize = properties.getBatchSize();
			
			// filterObj size is limited by max input size in text field
			SortedMap<String, String> filterObj = properties.getFilterObject(), cpy = null;
			cpy = filterObj;
			wrapper = SnowflakeOperationUtil.setupWrapper(properties);
			String tableName = SnowflakeOperationUtil.getTableName(properties);
			SortedMap<String, String> metadata = null;
			if (tableName != null) {
				metadata = SnowflakeOperationUtil.getTableMetadata(tableName,
						properties.getConnectionGetter().getConnection(properties.getLogger()),
						operationProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
						operationProperties.getProperty(SnowflakeOverrideConstants.SCHEMA));
			}
			executeUpdate(request, response, wrapper, batchSize, filterObj, cpy, metadata);
			if (batchSize > 1 && wrapper.getBatchedCount() != 0) {
				wrapper.executeQuery(batchSize);
			}
			
		} catch (Exception e) {
			throw new ConnectorException(e);
		} finally {
			/*
			 * closes wrappers if it's not null
			 * otherwise commit and close properties
			 */
			if (wrapper != null) {
				wrapper.close();
			}
			else {
				if(properties != null) {
					properties.commitAndClose();
				}
			}
		}
	}

	private void executeUpdate(UpdateRequest request, OperationResponse response, SnowflakeWrapper wrapper,
			Long batchSize, SortedMap<String, String> filterObj, SortedMap<String, String> cpy, SortedMap<String, String> metadata) {
		for (ObjectData requestData : request) {
			requestData.getLogger().info("Started processing");
			// convert from input stream to a JSON object
			SortedMap<String, String> dataObj = null;
			InputStream data = requestData.getData();
			try {
				dataObj = JSONHandler.readSortedMap(data);

				if (dataObj.isEmpty()) {
					throw new ConnectorException("Empty JSON object"); // TODO: handle empty objects
				}

				for (String keyStr : cpy.keySet()) {
					filterObj.put(keyStr, dataObj.get(keyStr));
					dataObj.remove(keyStr);
				}

				if (dataObj.size() == 0) {
					throw new ConnectorException(
							"No values provided to be set"); // TODO: handle setting values with null
				}
				if (wrapper.getPreparedStatement() == null) {
					wrapper.setPreparedStatement(wrapper.constructUpdateStatement(dataObj, filterObj));
				}
				ConnectionOverrideUtil.overrideConnectionWithDynamicProperties(
						wrapper.getPreparedStatement().getConnection(), requestData.getDynamicOperationProperties());
				// construct and fill a statement
				wrapper.fillStatementValuesWithDataType(wrapper.getPreparedStatement(), dataObj, filterObj, metadata);

				wrapper.executeHandler(batchSize);

				// a JSON File
				ResponseUtil.addSuccess(response, requestData, "0", ResponseUtil.toPayload(data));
			} catch (ConnectorException e) {
				String errorMessage = String.format(APPLICATION_ERROR_MESSAGE, wrapper.getCurrentBatch()) + e.getMessage();
				requestData.getLogger().log(Level.WARNING, errorMessage, e);
				response.addResult(requestData, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), errorMessage, ResponseUtil.toPayload(errorMessage));
			} catch (Exception e) {
				SnowflakeOperationUtil.handleGeneralException(response, requestData, e);
			} finally {
				IOUtil.closeQuietly(data);
			}
		}
	}

	/**
	 * gets the snowflake connection
	 */
	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}