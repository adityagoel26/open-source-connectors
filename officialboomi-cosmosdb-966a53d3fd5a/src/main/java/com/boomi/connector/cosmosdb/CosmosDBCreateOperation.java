//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import java.util.Map;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.cosmosdb.action.RetryableCreateOperation;
import com.boomi.connector.cosmosdb.util.SizeLimitedUpdateOperation;
/**
 * @author swastik.vn
 *
 */
public class CosmosDBCreateOperation extends SizeLimitedUpdateOperation {

	protected CosmosDBCreateOperation(CosmosDBConnection conn) {
		super(conn);
	}

/**
 * This method is to achieve CREATE operation of CosmosDB Connector. It also filters out
 * the request payload which are higher than 1 MB data.
 * 
 * @param UpdateRequest request
 * @param OperationResponse response
 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {

		Map<String, Object> inputConfig = getConnection().prepareInputConfig();
			try {
				getConnection().setOperationType("CREATE");
				RetryableCreateOperation operation = new RetryableCreateOperation(getConnection(),
						this.getContext().getObjectTypeId(), response, inputConfig, request);
				operation.execute();
			} catch (Exception e) {
				ResponseUtil.addExceptionFailures(response, request, e);
			}

	}


	/**
	 * Method to get the connection details
	 */
	@Override
	public CosmosDBConnection getConnection() {
		return (CosmosDBConnection) super.getConnection();
	}

}
