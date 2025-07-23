// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.wrappers.SnowflakeStoredProcedureBatcher;
import com.boomi.util.IOUtil;

import java.io.InputStream;
import java.util.ArrayList;

public class SnowflakeExecuteOperation extends SizeLimitedUpdateOperation {

	@SuppressWarnings("unchecked")
	public SnowflakeExecuteOperation(SnowflakeConnection conn) {
		super(conn);
	}


	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		response.getLogger().entering(this.getClass().getCanonicalName(), "executeUpdate()");
		ConnectionProperties properties = null;
		SnowflakeStoredProcedureBatcher batcher = null;
		try {
			properties = new ConnectionProperties(getConnection(),
					getContext().getOperationProperties(), getContext().getObjectTypeId(),
					response.getLogger());
			 batcher = new SnowflakeStoredProcedureBatcher(properties.getConnectionGetter(), properties.getConnectionTimeFormat(), properties.getLogger(), properties.getTableName(), properties.getBatchSize());
			ArrayList<ObjectData> requestDataArray = new ArrayList<>();
			for (ObjectData requestData : request) {
				requestData.getLogger().info("Started processing");
				requestDataArray.add(requestData);
				InputStream inputData = requestData.getData();
				handleBatchCall(response, batcher, requestDataArray, requestData, inputData);
			}

			if(!requestDataArray.isEmpty()) {
				// for last batch of documents with and without connection override
				batcher.commitAndRetrieveData(requestDataArray, response,requestDataArray.
						get(requestDataArray.size()-1).getDynamicOperationProperties());
			}
		}catch(Exception e) {
			throw new ConnectorException(e);
		}finally {
			
			/**
			 * closes batcher if it's not null
			 * otherwise commit and close properties
			 */
			
			if (batcher == null) {
				if (properties != null) {
					properties.commitAndClose();
				}
			}else {
				batcher.close();
			}
		}
		
	}

	private void handleBatchCall(OperationResponse response, SnowflakeStoredProcedureBatcher batcher,
			ArrayList<ObjectData> requestDataArray, ObjectData requestData, InputStream inputData) {
		try {
			batcher.addCall(JSONHandler.readSortedMap(inputData), requestDataArray,
					response,requestData.getDynamicOperationProperties());
		} catch (ConnectorException e) {
			SnowflakeOperationUtil.handleConnectorException(response, requestData, e);
		} catch (Exception e) {
			SnowflakeOperationUtil.handleGeneralException(response, requestData, e);
		} finally {
			IOUtil.closeQuietly(inputData);
		}
	}

	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}
