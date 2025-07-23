//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.action;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.cosmosdb.CosmosDBConnection;
import com.boomi.connector.cosmosdb.bean.DeleteOperationRequest;
import com.boomi.connector.cosmosdb.util.Timer;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.exception.CosmosDBRetryException;

/**
 * Implements logic for retryable delete operation
 * @author abhijit.d.mishra
 */
public class RetryableDeleteOperation extends RetryableAction {

	/**
	 * Instantiates a new retryable update operation.
	 *
	 * @param connection     the connection
	 * @param collectionName the collection name
	 * @param response       the response
	 * @param inputConfig    the input config
	 * @param request        the request
	 * @param charset        the charset
	 */
	public RetryableDeleteOperation(CosmosDBConnection connection, String collectionName, OperationResponse response,
			Map<String, Object> inputConfig, UpdateRequest request, Charset charset) {
		super(connection, collectionName, response, inputConfig,request);
	}

	
	/**
	 * Execute.
	 * @throws CosmosDBConnectorException 
	 */
	@Override
	public void execute() throws CosmosDBConnectorException {
		Iterable<ObjectData> request = getRequest();
		try (CloseableHttpClient clientConnection = HttpClientBuilder.create().build()) {
			deleteOperation(request, clientConnection);
		} catch (CosmosDBConnectorException e) {
			throw e;
		} catch (IOException e) {
			throw new CosmosDBConnectorException(e.getMessage());
		}
	}

	/**
	 * Request gets iterated and delete operation gets performed for 
	 * each record.
	 * @throws CosmosDBConnectorException 
	 */
	private void deleteOperation(Iterable<ObjectData> request, CloseableHttpClient clientConnection)
			throws CosmosDBConnectorException {
		CosmosDBPhasedRetry retry = new CosmosDBPhasedRetry(getMaxAllowedRetries(), getConnection());
		int numOfAttempts = 0;
		Exception retryFailedException = null;
		String objectId = null;
		for (ObjectData objectData : request) {
			numOfAttempts = 0;
			boolean shouldRetry = true;
			while (shouldRetry) {
				try {
					if (retryFailedException == null) {
						DeleteOperationRequest resulOperationRequest = doExecute(objectData, clientConnection);
						getConnection().addOperationResponse(false, getResponse(), null, objectData, false,
								resulOperationRequest.getId(),200, OperationType.DELETE.toString());
					} else {
						getConnection().addOperationResponse(false, getResponse(), retryFailedException, objectData,
								true, null, 500, OperationType.DELETE.toString());
					}
					break;
				} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
					shouldRetry = retry.shouldRetry(numOfAttempts, NULL_STATUS, ex);
					if (ex.getClass().equals(CosmosDBConnectorException.class))
						objectId = ((CosmosDBConnectorException) ex).getObjectId();
					if(numOfAttempts == retry.getMaxRetries() && !shouldRetry
							&& ex.getClass().equals(CosmosDBRetryException.class)) {
						retryFailedException = ex;
						getConnection().addOperationResponse(false, getResponse(), ex, objectData, true, objectId, 500, OperationType.DELETE.toString());
					} else {
						retryFailedException = null;
						getConnection().addOperationResponse(shouldRetry, getResponse(), ex, objectData, true, objectId, 500, OperationType.DELETE.toString());
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
	 * @param clientConnection 
	 *
	 * @param batch the batch
	 * @throws Exception
	 */
	DeleteOperationRequest doExecute(ObjectData request, CloseableHttpClient clientConnection) throws CosmosDBConnectorException, CosmosDBRetryException {
		return getConnection().doDelete(request, getCollectionName(), clientConnection);
	}
	
	@Override
	void doExecute() {
		throw new UnsupportedOperationException();
	}

}
