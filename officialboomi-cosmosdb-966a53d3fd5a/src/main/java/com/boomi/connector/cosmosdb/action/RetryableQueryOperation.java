//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.cosmosdb.action;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.cosmosdb.CosmosDBConnection;
import com.boomi.connector.cosmosdb.util.Timer;
import com.boomi.connector.exception.CosmosDBConnectorException;
import com.boomi.connector.exception.CosmosDBRetryException;

/**
 * Implements logic for retryable query operation
 * @author abhijit.d.mishra
 */
public class RetryableQueryOperation extends RetryableAction {

	/** The request. */
	private final FilterData request;

	private List<Entry<String, String>> filterParameters;

	/**
	 * Instantiates a new retryable Query operation.
	 *
	 * @param connection     the connection
	 * @param collectionName the collection name
	 * @param response       the response
	 * @param inputConfig    the input config
	 * @param request        the request
	 * @param charset        the charset
	 */
	public RetryableQueryOperation(CosmosDBConnection connection, FilterData request, String collectionName,
			OperationResponse response, Map<String, Object> inputConfig, List<Entry<String, String>> baseQueryTerms) {
		super(connection, collectionName, response, inputConfig);
		this.request = request;
		this.filterParameters = baseQueryTerms;
	}

	/**
	 * Execute.
	 * @throws CosmosDBConnectorException 
	 */
	@Override
	public void execute() throws CosmosDBConnectorException {
		CosmosDBPhasedRetry retry = new CosmosDBPhasedRetry(getMaxAllowedRetries(), getConnection());
		int numOfAttempts = 0;
		boolean shouldRetry = true;
		while (shouldRetry) {
			try {
				doExecute(getFilterData());
				break;
			} catch (CosmosDBConnectorException | CosmosDBRetryException ex) {
				shouldRetry = retry.shouldRetry(numOfAttempts, NULL_STATUS, ex);
				getConnection().addQueryOperationResponse(shouldRetry, getResponse(), ex, getFilterData());
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

	public FilterData getFilterData() {
		return request;
	}

	void doExecute(FilterData filterData) throws CosmosDBConnectorException, CosmosDBRetryException {
		getConnection().doQuery(getCollectionName(), getFilterParameters(), getResponse(), filterData);
	}

	private List<Entry<String, String>> getFilterParameters() {
		return this.filterParameters;
	}

	/**
	 * Do execute.
	 */
	@Override
	void doExecute() {
		throw new UnsupportedOperationException();
	}
}
