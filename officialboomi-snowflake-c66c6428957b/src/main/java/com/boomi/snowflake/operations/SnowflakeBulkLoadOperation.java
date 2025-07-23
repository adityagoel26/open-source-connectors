// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import java.io.InputStream;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.controllers.SnowflakeCreateController;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.util.IOUtil;

public class SnowflakeBulkLoadOperation extends BaseUpdateOperation{

	public SnowflakeBulkLoadOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * Performs bulk load operation
	 * If parameters are passed through dynamic operation properties then those values
	 * will take precedence over values set in operation properties
	 * @param request request given to execute bulk load operation
	 * @param response response given to execute bulk load operation
	 */
	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		ConnectionProperties properties = null;
		SnowflakeCreateController controller = null;
		try {
			properties = new ConnectionProperties(getConnection(), getContext().getOperationProperties(),
					getContext().getObjectTypeId(), response.getLogger());
			controller = new SnowflakeCreateController(properties);
			for (ObjectData requestData : request) {
				properties.setDynamicProperties(requestData.getDynamicOperationProperties());
				InputStream inputData = requestData.getData();
				requestData.getLogger().info("Parsing document");
				uploadData(response, controller, requestData, inputData, properties);
			}
		} catch(Exception e) {
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
				controller.closeResources();
			}
		}
	}

	private void uploadData(OperationResponse response, SnowflakeCreateController controller,
			ObjectData requestData, InputStream inputData, ConnectionProperties properties) {
		try {
			SnowflakeOperationUtil.validateProperties(properties);
			//upload the data
			controller.receive(inputData, requestData, properties);

			// successfully pass input to next shape
			ResponseUtil.addSuccess(response, requestData, "0", ResponseUtil.toPayload(inputData));
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
