//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import java.util.Map;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.cosmosdb.action.RetryableCreateOperation;
import com.boomi.connector.cosmosdb.util.SizeLimitedUpdateOperation;

import static com.boomi.connector.cosmosdb.util.CosmosDbConstants.UPSERT;

public class CosmosDBUpsertOperation extends SizeLimitedUpdateOperation {

	protected CosmosDBUpsertOperation(CosmosDBConnection conn) {
		super(conn);
	}

	/**
	 * This method is to achieve UPDATE operation of Comos DB Connector. It also filters out
	 * the request payloads which are higher than 1 MB data.
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		Map<String, Object> inputConfig = getConnection().prepareInputConfig();
		try {
			getConnection().setOperationType(UPSERT);
			RetryableCreateOperation operation = new RetryableCreateOperation(getConnection(),
					this.getContext().getObjectTypeId(), response, inputConfig, request);
			operation.execute();
		} catch (Exception e) {
			ResponseUtil.addExceptionFailures(response, request, e);
		}
		
	}
	
	@Override
	public CosmosDBConnection getConnection() {
		return (CosmosDBConnection) super.getConnection();
	}

}
