// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import java.io.InputStream;
import java.util.SortedMap;
import java.util.logging.Level;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.util.BaseGetOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.controllers.SnowflakeGetController;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.util.IOUtil;

public class SnowflakeGetOperation extends BaseGetOperation {

	/** The Constant SNOWFLAKE_BATCHING. */
	private static final String SNOWFLAKE_BATCHING = "documentBatching";

	public SnowflakeGetOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * This function is called when GET operation is called
	 */
	@Override
	protected void executeGet(GetRequest request, OperationResponse response) {
		request.getObjectId().getLogger().info("Started processing");

		ConnectionProperties properties = null;
		SnowflakeGetController controller = null;
		SortedMap<String, String> filterObj = null;
		InputStream batch = null;
		try {
			PropertyMap operationProperties = getContext().getOperationProperties();
			String cookie = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
			if(cookie != null && !cookie.isEmpty()) {
				SortedMap<String, String> cookieMap = JSONHandler.readSortedMap(cookie);
				if(cookieMap != null && !cookieMap.isEmpty()) {
					operationProperties.putIfAbsent(SNOWFLAKE_BATCHING, Boolean.valueOf(cookieMap.get(SNOWFLAKE_BATCHING)));
				}
			}
			properties = new ConnectionProperties(getConnection(),
					operationProperties, getContext().getObjectTypeId(),
					response.getLogger());
			properties.getLogger().fine("Parsing JSON filter parameter");
			filterObj = JSONHandler.readSortedMap(request.getObjectId().getObjectId());
			ObjectData objectData= (ObjectData) request.getObjectId();
			controller = new SnowflakeGetController(properties, filterObj, objectData);

			if (!controller.next()) {
				ResponseUtil.addEmptySuccess(response, request.getObjectId(), "0");
			}else {
				do {
					batch = controller.getNext();
	                response.addPartialResult(request.getObjectId(),
							OperationStatus.SUCCESS, "OK", "Succuss!",
							ResponseUtil.toPayload(batch));
	                
	                IOUtil.closeQuietly(batch);
				}while(controller.next());
				response.finishPartialResult(request.getObjectId());
			}

		}catch(ConnectorException e) {
			request.getObjectId().getLogger().log(Level.WARNING, e.getMessage(), e);
			response.addResult(request.getObjectId(), OperationStatus.APPLICATION_ERROR,
					e.getStatusCode(), e.getMessage(),
					ResponseUtil.toPayload(e.getMessage()));
		}catch(Exception e) {
			request.getObjectId().getLogger().log(Level.SEVERE, e.getMessage());
			ResponseUtil.addExceptionFailure(response, request.getObjectId(), e);
		}finally {
			IOUtil.closeQuietly(batch);
			/**
			 * closes controller if it's not null
			 * otherwise commit and close properties
			 */
			if (controller == null) {
				if(properties != null) {
					properties.commitAndClose();
				}	
			}
			else {
				controller.close();
			}
		}
	}

	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}