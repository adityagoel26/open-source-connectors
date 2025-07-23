//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.action;

import java.util.Map;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.cosmosdb.CosmosDBConnection;
import com.boomi.connector.cosmosdb.util.CosmosDbConstants;
import com.boomi.connector.exception.CosmosDBConnectorException;

/**
 * Base class for all retryable operations.
 * @author abhijit.d.mishra
 */
public abstract class RetryableAction {

	/** The Constant NULL_STATUS. */
	protected static final Object NULL_STATUS = null;

	/** The connection. */
	private final CosmosDBConnection connection;
	
	/** The collection name. */
	private final String collectionName;
	
	private Iterable<ObjectData> request;
	
	/** The response. */
	private OperationResponse response;
	
	/** The input config. */
	private Map<String, Object> inputConfig;
	
	/**
	 * Instantiates a new retryable action.
	 *
	 * @param connection the connection
	 * @param collectionName the collection name
	 * @param response the response
	 * @param inputConfig the input config
	 */
	RetryableAction(CosmosDBConnection connection, String collectionName, OperationResponse response,
			Map<String, Object> inputConfig, Iterable<ObjectData> request) {
		this.connection = connection;
		this.response = response;
		this.collectionName = collectionName;
		this.inputConfig = inputConfig;
		this.request = request;
	}
	
	/**
	 * Instantiates a new retryable action.
	 *
	 * @param connection the connection
	 * @param collectionName the collection name
	 * @param response the response
	 */
	RetryableAction(CosmosDBConnection connection, String collectionName, OperationResponse response,  Iterable<ObjectData> request) {
		this.connection = connection;
		this.response = response;
		this.collectionName = collectionName;
		this.request = request;
	}
	
	/**
	 * Instantiates a new retryable action.
	 *
	 * @param connection the connection
	 * @param collectionName the collection name
	 * @param response the response
	 * @param inputConfig the input config
	 */
	RetryableAction(CosmosDBConnection connection, String collectionName, OperationResponse response,
			Map<String, Object> inputConfig) {
		this.connection = connection;
		this.response = response;
		this.collectionName = collectionName;
		this.inputConfig = inputConfig;
	}
	

	/**
	 * Gets the max allowed retries.
	 *
	 * @return the max allowed retries
	 */
	public int getMaxAllowedRetries() {
		return (int) getInputConfig().get(CosmosDbConstants.QUERY_MAXRETRY);
	}

	/**
	 * Execute.
	 * @throws CosmosDBConnectorException 
	 */
	abstract void execute() throws CosmosDBConnectorException;

	/**
	 * Do execute.
	 */
	abstract void doExecute();

	/**
	 * Gets the response.
	 *
	 * @return the response
	 */
	public OperationResponse getResponse() {
		return response;
	}

	/**
	 * Sets the response.
	 *
	 * @param response the new response
	 */
	public void setResponse(OperationResponse response) {
		this.response = response;
	}

	/**
	 * Gets the null status.
	 *
	 * @return the null status
	 */
	public static Object getNullStatus() {
		return NULL_STATUS;
	}

	/**
	 * Gets the connection.
	 *
	 * @return the connection
	 */
	public CosmosDBConnection getConnection() {
		return connection;
	}

	/**
	 * Gets the collection name.
	 *
	 * @return the collection name
	 */
	public String getCollectionName() {
		return collectionName;
	}

	/**
	 * Gets the input config.
	 *
	 * @return the input config
	 */
	public Map<String, Object> getInputConfig() {
		return inputConfig;
	}

	public Iterable<ObjectData> getRequest() {
		return request;
	}

}
