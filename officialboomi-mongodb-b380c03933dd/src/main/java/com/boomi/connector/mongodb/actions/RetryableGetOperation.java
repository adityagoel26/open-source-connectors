// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.actions;

import java.util.logging.Logger;

import org.bson.Document;

import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.mongodb.MongoDBConnectorBrowser;
import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.boomi.connector.mongodb.exception.MongoDBConnectException;

/**
 * Implements logic for retryable get operation.
 *
 */
public class RetryableGetOperation extends RetryableAction {

	/** The browse context. */
	MongoDBConnectorBrowser browseContext;

	/** The obj id. */
	private final String objId;
	
	private final String dataType;

	/** The id. */
	private final ObjectIdData id;

	/** The object type id. */
	private final String objectTypeId;

	/** The request. */
	GetRequest request;

	/** The logger. */
	Logger logger;
	

	/**
	 * Instantiates a new retryable get operation.
	 *
	 * @param connection        the connection
	 * @param request           the request
	 * @param objId             the obj id
	 * @param objectTypeId      the object type id
	 * @param response          the response
	 * @param id                the id
	 */
	public RetryableGetOperation(MongoDBConnectorConnection connection, GetRequest request, String objId, String objectTypeId, OperationResponse response, ObjectIdData id, String dataType) {
		super(connection, objectTypeId, response);
		this.objId = objId;
		this.dataType = dataType;
		this.objectTypeId = objectTypeId;
		this.request = request;
		this.id = id;
		logger = response.getLogger();
	}

	/**
	 * Calls the method {@linkdoGet()}which fetches the documents from the
	 * collection based on the objectId provided.
	 * @return the document
	 * @throws MongoDBConnectException the mongo DB connect exception
	 */
	Document doExecuteforGet() throws MongoDBConnectException {
        return getConnection().doGet(objectTypeId,objId,dataType);
    }

	/**
	 * Executes Get operation.
	 */
	@Override
	public void execute() {
		Document doc = null;
		Exception ex = null;

			try {
					doc = doExecuteforGet();
			} catch (Exception e) {
				ex = e;
				
			} finally {
				
					getConnection().updateOperationResponseforGet(getRequest(), getResponse(),
							getConnection().processErrorForGet(ex), doc, objId, id, dataType);
			

			}
		
	}

	/**
	 * Gets the request.
	 *
	 * @return the request
	 */
	public GetRequest getRequest() {
		return request;
	}


	/**
	 * overriden method of RetryableAction.
	 */
	@Override
	void doExecute() {
		throw new UnsupportedOperationException(
				"The method doExecuteforGet() is added instead of implemeting the overriden method");
	}

}
