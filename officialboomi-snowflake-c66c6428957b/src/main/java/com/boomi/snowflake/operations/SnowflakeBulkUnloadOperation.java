// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import java.io.InputStream;
import java.util.SortedMap;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.controllers.SnowflakeGetController;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.util.IOUtil;

public class SnowflakeBulkUnloadOperation extends SizeLimitedUpdateOperation{

	@SuppressWarnings("unchecked")
	public SnowflakeBulkUnloadOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * Performs bulk unload operation
	 * If parameters are passed through dynamic operation properties then those values
	 * will take precedence over values set in operation properties
	 * @param request request given to execute bulk unload operation
	 * @param response response given to execute bulk unload operation
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		ConnectionProperties properties = null;
		SnowflakeGetController controller = null;
		try {
			properties = new ConnectionProperties(getConnection(), getContext().getOperationProperties(),
					getContext().getObjectTypeId(), response.getLogger());
			for (ObjectData requestData : request) {
				properties.setDynamicProperties(requestData.getDynamicOperationProperties());
				requestData.getLogger().info("Started processing");
				controller = processSizeLimitedUpdate(requestData, response, properties);
			}
		} catch (Exception e) {
			throw new ConnectorException(e);
		} finally {
			/*
			 * finalize and closes controller if it's not null
			 * otherwise commit and close properties
			 */
			if (controller == null) {
				if (properties != null) {
					properties.commitAndClose();
				}
			} else {
				controller.close();
			}
		}
	}

	private SnowflakeGetController processSizeLimitedUpdate(ObjectData requestData, OperationResponse response,
			ConnectionProperties properties) {
		InputStream inputData = requestData.getData();
		InputStream file = null;
		SnowflakeGetController controller = null;
		try {
			SnowflakeOperationUtil.validateProperties(properties);
			properties.getLogger().fine("Parsing JSON filter parameter");
			SortedMap<String, String> filterSortedMap = JSONHandler.readSortedMap(inputData);

			controller = new SnowflakeGetController(properties, filterSortedMap, requestData);

			file = controller.getNextFile();

			if (file == null) {
				ResponseUtil.addEmptySuccess(response, requestData, "0");
			} else {
				do {
					ResponseUtil.addPartialSuccess(response, requestData, " ", ResponseUtil.toPayload(file));
					IOUtil.closeQuietly(file);
				} while ((file = controller.getNextFile()) != null);
				response.finishPartialResult(requestData);
			}
		} catch(ConnectorException e) {
			SnowflakeOperationUtil.handleConnectorException(response, requestData, e);
		} catch(Exception e) {
			SnowflakeOperationUtil.handleGeneralException(response, requestData, e);
		} finally {
			IOUtil.closeQuietly(inputData);
			IOUtil.closeQuietly(file);
		}
		return controller;
	}
	
	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}
