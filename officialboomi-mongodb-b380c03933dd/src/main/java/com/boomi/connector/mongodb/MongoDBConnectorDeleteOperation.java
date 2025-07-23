// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.mongodb.actions.RetryableDeleteOperation;
import com.boomi.connector.util.BaseDeleteOperation;
/**
 * Implements logic for the MongoDB Delete Operation.
 *
 */
public class MongoDBConnectorDeleteOperation extends BaseDeleteOperation {

	/**
	 * Instantiates a new mongo DB connector delete operation.
	 *
	 * @param conn the conn
	 */
	protected MongoDBConnectorDeleteOperation(MongoDBConnectorConnection conn) {
		super(conn);
	}

	/**
	 * logic to execute delete operation .
	 *
	 * @param request the request
	 * @param response the response
	 */
	@Override
	protected void executeDelete(DeleteRequest request, OperationResponse response) {
		Map<String, Object> inputConfig = getConnection().prepareInputConfig(this.getContext(), response.getLogger());
		String objectTypeId = this.getContext().getObjectTypeId();
		try {
			RetryableDeleteOperation deleteOp = new RetryableDeleteOperation(getConnection(), request,
					inputConfig, objectTypeId, response, this.getContext().getConfig(),StandardCharsets.UTF_8);
			deleteOp.execute();
		}finally{
			getConnection().closeConnection();
		}
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 */
	@Override
    public MongoDBConnectorConnection getConnection() {
        return (MongoDBConnectorConnection) super.getConnection();
    }
}