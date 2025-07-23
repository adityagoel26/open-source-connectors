//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.cosmosdb.action.RetryableDeleteOperation;
import com.boomi.connector.util.BaseConnection;
import com.boomi.connector.util.BaseUpdateOperation;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBDeleteExecuteOperation extends BaseUpdateOperation{

	protected CosmosDBDeleteExecuteOperation(BaseConnection conn) {
		super(conn);
	}
	
	@Override
	public CosmosDBConnection getConnection() {
		return (CosmosDBConnection) super.getConnection();
	}

	/**
	 * This method is used to achieve the DELETE operation of Cosmos DB connector.
	 */
	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		Map<String, Object> inputConfig = getConnection().prepareInputConfig();
		try {
			RetryableDeleteOperation operation = new RetryableDeleteOperation(getConnection(),
					this.getContext().getObjectTypeId(), response, inputConfig, request,
					StandardCharsets.UTF_8);
			operation.execute();
		}catch(Exception ex) {
			ResponseUtil.addExceptionFailures(response, request, new ConnectorException(ex.getMessage()));
		}
	}

}
