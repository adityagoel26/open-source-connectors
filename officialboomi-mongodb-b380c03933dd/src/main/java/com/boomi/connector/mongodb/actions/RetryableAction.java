// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.actions;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.mongodb.BatchDocuments;
import com.boomi.connector.mongodb.InputWrapper;
import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.boomi.connector.mongodb.ObjectIdDataBatchWrapper;
import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;
/**
 * Base class for all retryable operations.
 */
public abstract class RetryableAction {

	/** The Constant NULL_STATUS. */
	protected static final Object NULL_STATUS = null;

	/** The connection. */
	private final MongoDBConnectorConnection connection;
	
	/** The input wrapper. */
	private InputWrapper inputWrapper;
	
	/** The collection name. */
	private final String collectionName;
	
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
	RetryableAction(MongoDBConnectorConnection connection, String collectionName, OperationResponse response,
			Map<String, Object> inputConfig) {
		this.connection = connection;
		this.response = response;
		this.collectionName = collectionName;
		this.inputConfig = inputConfig;
	}
	
	/**
	 * Instantiates a new retryable action.
	 *
	 * @param connection the connection
	 * @param collectionName the collection name
	 * @param response the response
	 */
	RetryableAction(MongoDBConnectorConnection connection, String collectionName, OperationResponse response) {
		this.connection = connection;
		this.response = response;
		this.collectionName = collectionName;
	}
	
	/**
	 * Clears the batch.
	 *
	 * @param batch the batch
	 */
	public void clearBatch(List<TrackedDataWrapper> batch) {
		InputWrapper inputWrap = getInputWrapper();
		if (inputWrap instanceof BatchDocuments) {
			for (TrackedDataWrapper item : batch) {
				item.setObjectId(item.getDoc().get(MongoDBConstants.ID_FIELD_NAME));
				item.getDoc().clear();
				item.setDoc(null);
			}
		} else if (inputWrap instanceof ObjectIdDataBatchWrapper) {
			for (TrackedDataWrapper item : batch) {
				item.setObjectId(null);
			}

		}
	}
	
	/**
	 * Process the provided input into batches.
	 *
	 * @param request the request
	 * @param atomConfig the atom config
	 * @param charset the charset
	 */
	public void processInputIntoBatches(Iterable<ObjectData> request, AtomConfig atomConfig, Charset charset) {
		setInputWrapper(new BatchDocuments(request, (int) getInputConfig().get(MongoDBConstants.QUERY_BATCHSIZE),
				atomConfig, charset, getResponse(),(boolean) getInputConfig().get(MongoDBConstants.INCLUDE_SIZE_EXCEEDED_PAYLOAD)));
	}

	/**
	 * Process input into batches.
	 *
	 * @param request the request
	 * @param atomConfig the atom config
	 * @param rsponse 
	 * @param charset the charset
	 */
	public void processInputIntoBatches(DeleteRequest request, AtomConfig atomConfig, OperationResponse rsponse, Charset charset) {
		setInputWrapper(new ObjectIdDataBatchWrapper(request,
				(int) getInputConfig().get(MongoDBConstants.QUERY_BATCHSIZE),rsponse,atomConfig));
	}

	/**
	 * Execute.
	 * @throws MongoDBConnectException 
	 */
	abstract void execute();

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
	public MongoDBConnectorConnection getConnection() {
		return connection;
	}

	/**
	 * Gets the input wrapper.
	 *
	 * @return the input wrapper
	 */
	public InputWrapper getInputWrapper() {
		return inputWrapper;
	}

	/**
	 * Sets the input wrapper.
	 *
	 * @param inputWrapper the new input wrapper
	 */
	public void setInputWrapper(InputWrapper inputWrapper) {
		this.inputWrapper = inputWrapper;
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

}
