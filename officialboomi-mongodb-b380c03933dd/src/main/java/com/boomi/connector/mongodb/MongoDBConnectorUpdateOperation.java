// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.mongodb.util.UpdateUpsertUtil;
import com.boomi.connector.util.BaseUpdateOperation;
/**
 * Implements a logic for performing update operation on mongoDB
 * 
 */
public class MongoDBConnectorUpdateOperation extends BaseUpdateOperation {

	/**
	 * Instantiates a new mongo DB connector update operation.
	 *
	 * @param conn the connection
	 */
	protected MongoDBConnectorUpdateOperation(MongoDBConnectorConnection conn) {
		super(conn);
	}

	/**
	 * logic for executing the update operation.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		UpdateUpsertUtil.executeUpdateUpsertOperation(this.getContext(), getConnection(), request, response);
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