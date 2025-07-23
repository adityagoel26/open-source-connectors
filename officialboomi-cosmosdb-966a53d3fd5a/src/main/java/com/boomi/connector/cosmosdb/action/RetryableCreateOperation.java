//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.action;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.cosmosdb.CosmosDBConnection;
import com.boomi.connector.cosmosdb.bean.CreateOperationRequest;
import com.boomi.connector.cosmosdb.util.Timer;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.exception.CosmosDBRetryException;

/**
 * @author swastik.vn
 *
 */
public class RetryableCreateOperation extends RetryableAction {
	
	
	public RetryableCreateOperation(CosmosDBConnection connection, String collectionName, OperationResponse response,
			Map<String, Object> inputConfig, Iterable<ObjectData> request) {
		super(connection, collectionName, response, inputConfig, request);
	}

	private static final Logger logger = Logger.getLogger(RetryableCreateOperation.class.getName());
	
	/**
	 * Retry Logic for create operation
	 * @throws CosmosDBConnectorException
	 */
	@Override
	public void execute() throws CosmosDBConnectorException {
		CosmosDBPhasedRetry retry = new CosmosDBPhasedRetry(getMaxAllowedRetries(), getConnection());
		int numOfAttempts = 0;
		String objectId = null;
		Exception retryFailedException = null;
		Iterable<ObjectData> request = getRequest();
			for (ObjectData objectData : request) {
				numOfAttempts = 0;
				boolean shouldRetry = true;
				
				
				while (shouldRetry) {
					try {
						if (retryFailedException == null) {
							CreateOperationRequest resulOperationRequest = doExecute(objectData);
							getConnection().addOperationResponse(false, getResponse(), null, objectData, false,
									resulOperationRequest.getRequestId(), resulOperationRequest.getStatusCode(), getConnection().getOperationType());
							logger.log(Level.FINE, "Create Operation Executed Successfully");
						} else {
							getConnection().addOperationResponse(false, getResponse(), retryFailedException,
									objectData, true, null, 500, getConnection().getOperationType());
						}
						break;
					} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
						shouldRetry = retry.shouldRetry(numOfAttempts, NULL_STATUS, ex);
						if (ex.getClass().equals(CosmosDBConnectorException.class))
							objectId = ((CosmosDBConnectorException) ex).getObjectId();
						if (numOfAttempts == retry.getMaxRetries() && !shouldRetry
								&& ex.getClass().equals(CosmosDBRetryException.class)) {
							retryFailedException = ex;
							getConnection().addOperationResponse(false, getResponse(), ex, objectData, true,
									objectId, 500, getConnection().getOperationType());
						} else {
							retryFailedException = null;
							getConnection().addOperationResponse(shouldRetry, getResponse(), ex, objectData, true,
									objectId, 500, getConnection().getOperationType());
						}
						retry.logRetryAttempt(numOfAttempts, null != ex, shouldRetry, getResponse().getLogger());
						if (shouldRetry) {
							Timer.start();
							retry.backoff(numOfAttempts);
							getResponse().getLogger()
									.info(new StringBuffer("Backoff: ").append(Timer.stop())
											.append("ms completed. Proceeding with attempt:").append(numOfAttempts + 1)
											.toString());
						}
						numOfAttempts++;
					}
					
					
				}
			}
	}
	/**
	 * doExecute method which calls the doCreate method in CosmosDBConnection
	 * 
	 * @param request
	 * @return CreateOperationRequest request
	 * @throws CosmosDBConnectorException
	 * @throws CosmosDBRetryException
	 */
	CreateOperationRequest doExecute(ObjectData request) throws CosmosDBConnectorException, CosmosDBRetryException {
		String opType = getConnection().getOperationType();
		if( opType!= null && opType.equals(OperationType.UPSERT.toString())) {
			return getConnection().doUpsert(request, getCollectionName());
		}
		return getConnection().doCreate(request, getCollectionName());
	}

	@Override
	void doExecute() {
		throw new UnsupportedOperationException();
		
	}

}
