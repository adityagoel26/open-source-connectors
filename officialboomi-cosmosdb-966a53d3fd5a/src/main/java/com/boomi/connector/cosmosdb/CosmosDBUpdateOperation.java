//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.cosmosdb.action.RetryableUpdateOperation;
import com.boomi.connector.cosmosdb.util.SizeLimitedUpdateOperation;

public class CosmosDBUpdateOperation extends SizeLimitedUpdateOperation {

	protected CosmosDBUpdateOperation(CosmosDBConnection conn) {
		super(conn);
	}

	@Override
	public CosmosDBConnection getConnection() {
		return (CosmosDBConnection) super.getConnection();
	}

	/**
	 * This method is to achieve UPDATE operation of Comos DB Connector. It also filters out
	 * the request payloads which are higher than 1 MB data.
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		Map<String, Object> inputConfig = getConnection().prepareInputConfig();
		try {
			RetryableUpdateOperation operation = new RetryableUpdateOperation(getConnection(),
					this.getContext().getObjectTypeId(), response, inputConfig, this.getContext().getConfig(), request,
					StandardCharsets.UTF_8);
			operation.execute();
		}catch(Exception ex) {
			ResponseUtil.addExceptionFailures(response, request, ex);
		}
		
	}

}