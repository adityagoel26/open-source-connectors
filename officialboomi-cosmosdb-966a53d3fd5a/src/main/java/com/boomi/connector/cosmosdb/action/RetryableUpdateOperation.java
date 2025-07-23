//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.action;

import java.nio.charset.Charset;
import java.util.Map;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.cosmosdb.CosmosDBConnection;
import com.boomi.connector.cosmosdb.bean.UpdateOperationRequest;
import com.boomi.connector.cosmosdb.util.Timer;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.exception.CosmosDBRetryException;

/**
 * Implements logic for retryable update operation
 * @author abhijit.d.mishra
 */
public class RetryableUpdateOperation extends RetryableAction {

	/**
	 * Instantiates a new retryable update operation.
	 *
	 * @param connection     the connection
	 * @param collectionName the collection name
	 * @param response       the response
	 * @param inputConfig    the input config
	 * @param atomConfig     the atom config
	 * @param request        the request
	 * @param charset        the charset
	 */
	public RetryableUpdateOperation(CosmosDBConnection connection, String collectionName, OperationResponse response,
			Map<String, Object> inputConfig, AtomConfig atomConfig, Iterable<ObjectData> request, Charset charset) {
		super(connection, collectionName, response, inputConfig, request);
	}

	/**
	 * Execute.
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
						UpdateOperationRequest resulOperationRequest = doExecute(objectData);
						getConnection().addOperationResponse(false, getResponse(), null, objectData, false,
								resulOperationRequest.getId(),200, OperationType.UPDATE.toString());
					} else {
						getConnection().addOperationResponse(false, getResponse(), retryFailedException, objectData,
								true, null,500, OperationType.UPDATE.toString());
					}
					break;
				} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
					shouldRetry = retry.shouldRetry(numOfAttempts, NULL_STATUS, ex);
					if (ex.getClass().equals(CosmosDBConnectorException.class))
						objectId = ((CosmosDBConnectorException) ex).getObjectId();
					if (numOfAttempts == retry.getMaxRetries() && !shouldRetry
							&& ex.getClass().equals(CosmosDBRetryException.class)) {
						retryFailedException = ex;
						getConnection().addOperationResponse(false, getResponse(), ex, objectData, true, objectId,500, OperationType.UPDATE.toString());
					} else {
						retryFailedException = null;
						getConnection().addOperationResponse(shouldRetry, getResponse(), ex, objectData, true,
								objectId, 500, OperationType.UPDATE.toString());
					}
					retry.logRetryAttempt(numOfAttempts, null != ex, shouldRetry, getResponse().getLogger());
					if (shouldRetry) {
						Timer.start();
						retry.backoff(numOfAttempts);
						getResponse().getLogger().info(new StringBuffer("Backoff: ").append(Timer.stop())
								.append("ms completed. Proceeding with attempt:").append(numOfAttempts + 1).toString());
					}
					numOfAttempts++;
				}
			}
		}
	}

	/**
	 * Do execute.
	 *
	 * @param batch the batch
	 * @throws Exception
	 */
	UpdateOperationRequest doExecute(ObjectData request) throws CosmosDBConnectorException, CosmosDBRetryException {
		return getConnection().doUpdate(request, getCollectionName());
	}

	/**
	 * Do execute.
	 */
	@Override
	void doExecute() {
		throw new UnsupportedOperationException();
	}

}
