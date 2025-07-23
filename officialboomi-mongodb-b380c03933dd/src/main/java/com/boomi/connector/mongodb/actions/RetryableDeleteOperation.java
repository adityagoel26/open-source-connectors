// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.actions;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.boomi.connector.mongodb.ObjectIdDataBatchWrapper;
import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.bean.BatchResult;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
import com.mongodb.MongoException;

/**
 * Implements logic for retryable delete operation
 * 
 */
public class RetryableDeleteOperation extends RetryableAction {

	/** The object type id. */
	private final String objectTypeId;

	/** The input wrapper. */
	ObjectIdDataBatchWrapper inputWrapper;

	/**
	 * Instantiates a new retryable delete operation.
	 *
	 * @param connection   the connection
	 * @param request      the request
	 * @param inputConfig  the input config
	 * @param objectTypeId the object type id
	 * @param response     the response
	 * @param config       the config
	 * @param charset      the charset
	 */
	public RetryableDeleteOperation(MongoDBConnectorConnection connection, DeleteRequest request,
			Map<String, Object> inputConfig, String objectTypeId, OperationResponse response, AtomConfig config,
			Charset charset) {
		super(connection, objectTypeId, response, inputConfig);
		this.objectTypeId = objectTypeId;
		processInputIntoBatches(request, config,response,charset);
	}

	/**
	 * Executes Delete operation.
	 * @throws MongoDBConnectException 
	 */
	@Override
	public void execute(){
		Exception ex = null;
		inputWrapper = (ObjectIdDataBatchWrapper) getInputWrapper();
		List<TrackedDataWrapper> batch = null;
		BatchResult unsuccessfulRecordsResult = new BatchResult();
		while (inputWrapper.hasNext()) {
			batch = inputWrapper.next();
			unsuccessfulRecordsResult.reset();
				try {
					doExecute(batch);
				} catch (Exception e) {
					ex = e;
				} finally {
					getConnection().updateOperationResponse(getResponse(), ex, batch,
							inputWrapper, unsuccessfulRecordsResult);
				}
		}

	}

	/**
	 * Its calls the method @link doDelete(batch, objectTypeId, inputWrapper,
	 * getResponse()) which deletes documents from the collection,based on the
	 * input.
	 *
	 * @param batch the batch
	 * @throws MongoException the mongo exception
	 */
	void doExecute(List<TrackedDataWrapper> batch) {
		getConnection().doDelete(batch, objectTypeId);
	}

	/**
	 * overidden method of RetryableAction.
	 */
	@Override
	void doExecute() {
		throw new UnsupportedOperationException(
				"The method doExecute(List<TrackedDataWrapper> batch) is added instead of implementing overriden method");
	}

}
