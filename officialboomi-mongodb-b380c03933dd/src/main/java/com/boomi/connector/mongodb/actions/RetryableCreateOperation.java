// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.actions;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.mongodb.BatchDocuments;
import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.bean.BatchResult;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.mongodb.MongoException;

/**
 * Implement logic for retryable create operation.
 *
 */
public class RetryableCreateOperation extends RetryableAction {

	/**
	 * Instantiates a new retryable create operation.
	 *
	 * @param connection     the connection
	 * @param collectionName the collection name
	 * @param response       the response
	 * @param inputConfig    the input config
	 * @param atomConfig     the atom config
	 * @param request        the request
	 * @param charset        the charset
	 */
	public RetryableCreateOperation(MongoDBConnectorConnection connection, String collectionName,
			OperationResponse response, Map<String, Object> inputConfig, AtomConfig atomConfig,
			Iterable<ObjectData> request, Charset charset) {
		super(connection, collectionName, response, inputConfig);
		processInputIntoBatches(request, atomConfig, charset);
	}

	/**
	 * Executes the create operation.
	 * @throws MongoDBConnectException 
	 */
	@Override
	public void execute(){
		Exception ex = null;
		List<TrackedDataWrapper> batch = null;
		BatchDocuments input = (BatchDocuments) getInputWrapper();
		BatchResult unsuccessfulRecordsResult = new BatchResult();
		while (input.hasNext())
		{
					batch = input.next();
					unsuccessfulRecordsResult.reset();
					try {
						doExecute(batch);

					} catch (Exception e) {
						ex = e;
					} finally {
						getConnection().updateOperationResponse(getResponse(), ex, batch, input, unsuccessfulRecordsResult);
						ex = null;
					}
		}
	}

	/**
	 * Its calls the method @link doCreate(batch,getCollectionName()) which inserts
	 * documents into the collection,based on the input.
	 *
	 * @param batch the batch
	 * @throws MongoException the mongo exception
	 */
	void doExecute(List<TrackedDataWrapper> batch) {
		getConnection().doCreate(batch, getCollectionName());
	}

	/**
	 * overridden method of the RetrybaleAction.
	 */
	@Override
	void doExecute() {
		throw new UnsupportedOperationException(
				"The method doExecute(List<TrackedDataWrapper> batch) is added instead of implementing overriden method ");
	}

}
