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
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.mongodb.MongoException;

/**
 * Implements logic for retryable update upsert operation
 *
 */
public class RetryableUpdateUpsertOperation extends RetryableAction {

	/**
	 * Instantiates a new retryable update upsert operation.
	 *
	 * @param connection     the connection
	 * @param collectionName the collection name
	 * @param response       the response
	 * @param inputConfig    the input config
	 * @param atomConfig     the atom config
	 * @param request        the request
	 * @param charset        the charset
	 */
	public RetryableUpdateUpsertOperation(MongoDBConnectorConnection connection, String collectionName,
			OperationResponse response, Map<String, Object> inputConfig, AtomConfig atomConfig,
			Iterable<ObjectData> request, Charset charset) {
		super(connection, collectionName, response, inputConfig);
		processInputIntoBatches(request, atomConfig, charset);
	}

	/**
	 * Executes the update/upsert operation and if there is an
	 * exception{@linkisErrorRecoverable(exception)}
	 * @throws MongoDBConnectException 
	 */
	@Override
	public void execute() {
		Exception ex = null;
		BatchDocuments input = (BatchDocuments) getInputWrapper();
		List<TrackedDataWrapper> batch = null;
		BatchResult unsuccessfulRecordsResult = new BatchResult();
		while (input.hasNext()) {
			batch = input.next();
			unsuccessfulRecordsResult.reset();
			try {
				doExecute(batch);

			} catch (Exception e) {
				ex = e;
				if (batch.size() == 1 && !getConnection().isBatchFailed(e)) {
					int failedRecIndex = 0;
					unsuccessfulRecordsResult.getFailedRecIndexes().add(failedRecIndex);
					unsuccessfulRecordsResult.getFailedRecords().add(batch.get(failedRecIndex));
				}
			} finally {
				getConnection().updateOperationResponse(getResponse(), ex, batch, input,
						unsuccessfulRecordsResult);
			}
		}
	}

	/**
	 * Calls the {@linkdoModify()} which actually performs an update or upsert
	 * operation based on the selected operation type by the user.
	 *
	 * @param batch
	 * @throws MongoException
	 */
	void doExecute(List<TrackedDataWrapper> batch) throws MongoException {
		getConnection().doModify(batch, getCollectionName(),
		(boolean) getInputConfig().get(MongoDBConstants.IS_UPSERT_OPERATION));
		}

	/**
	 * Do execute.
	 */
	@Override
	void doExecute() {
		throw new UnsupportedOperationException();
	}

}
