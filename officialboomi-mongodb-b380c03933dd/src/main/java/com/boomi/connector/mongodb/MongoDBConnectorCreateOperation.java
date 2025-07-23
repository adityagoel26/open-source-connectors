// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.mongodb.actions.RetryableCreateOperation;
import com.boomi.connector.util.BaseUpdateOperation;
/**
 * The Class MongoDBConnectorCreateOperation.
 * 
 */
public class MongoDBConnectorCreateOperation extends BaseUpdateOperation {

	/**
	 * Instantiates a new mongo DB connector for CREATE operation.
	 *
	 * @param conn the conn
	 */
	protected MongoDBConnectorCreateOperation(MongoDBConnectorConnection conn) {
		super(conn);
	}
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.util.BaseUpdateOperation#executeUpdate(com.boomi.
	 * connector.api.UpdateRequest, com.boomi.connector.api.OperationResponse)
	 */
	@Override
	protected void executeUpdate(UpdateRequest request, OperationResponse response) {
		List<ObjectData> trackedData = new ArrayList<>();
		for(ObjectData objData : request) {
			trackedData.add(objData);
		}
		try {
			Map<String, Object> inputConfig = getConnection().prepareInputConfig(this.getContext(), response.getLogger());
			RetryableCreateOperation createOp = new RetryableCreateOperation(getConnection(),
					this.getContext().getObjectTypeId(), response, inputConfig, this.getContext().getConfig(), trackedData,
					StandardCharsets.UTF_8);
			createOp.execute();
		}catch(Exception e){
			ResponseUtil.addExceptionFailures(response, trackedData, e);
		}finally {
			getConnection().closeConnection();
		}

	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 */
	/*
	 * (non-Javadoc)
	 * 
	 * @see com.boomi.connector.util.BaseOperation#getConnection()
	 */
	@Override
	public MongoDBConnectorConnection getConnection() {
		return (MongoDBConnectorConnection) super.getConnection();
	}
}