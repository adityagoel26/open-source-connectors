// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.actions;

import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.mongodb.MongoDBConnectorConnection;
import com.boomi.connector.mongodb.constants.MongoDBConstants;
import com.boomi.util.IOUtil;
import com.mongodb.client.MongoCursor;

/**
 * Implements logic for retryable query operation.
 * 
 */
public class RetryableQueryOperation extends RetryableAction {

	/** The request. */
	private final FilterData request;

	/** The bson filter. */
	private final Bson bsonFilter;

	/** The bsonprojection. */
	private final Bson bsonprojection;

	/** The sort spec. */
	private final Bson sortSpec;

	/** The query result cursor. */
	private MongoCursor<Document> queryResultCursor;

	/** The OperationContext type. */
	private OperationContext operationContext;

	/**
	 * Instantiates a new retryable query operation.
	 *
	 * @param request        the request
	 * @param connection     the connection
	 * @param collectionName the collection name
	 * @param response       the response
	 * @param inputConfig    the input config
	 * @param objectIdType   the objectIdType
	 */
	public RetryableQueryOperation(FilterData request, MongoDBConnectorConnection connection, String collectionName,
			OperationResponse response, Map<String, Object> inputConfig, String objectIdType,
			OperationContext operationContext) {
		super(connection, collectionName, response, inputConfig);
		this.request = request;
		this.operationContext = operationContext;
		bsonFilter = (Bson) getInputConfig().get(MongoDBConstants.QUERY_FILTER);
		bsonprojection = (Bson) getInputConfig().get(MongoDBConstants.QUERY_PROJECTION);
		sortSpec = (Bson) getInputConfig().get(MongoDBConstants.SORT_SPEC);
	}

	/**
	 * Executes Query operation 
	 */
	@Override
	public void execute(){
		Exception ex = null;
		try {
			doExecute();
		} catch (Exception e) {
			ex = e;
		} finally {
				getConnection().updateQueryResponse(queryResultCursor, getRequest(), getResponse(),
						getConnection().processQueryError(ex),operationContext);
			if (null != queryResultCursor) {
				IOUtil.closeQuietly(queryResultCursor);
			}
		}
	}

	/**
	 * Calls the method {@linkdoQuery()}which queries the collection based on the
	 * objectId provided.
	 */
	@Override
	void doExecute() {
		queryResultCursor = getConnection().doQuery(getCollectionName(), getBsonFilter(), getBsonprojection(),
				getSortSpec(), (int) getInputConfig().get(MongoDBConstants.QUERY_BATCHSIZE));
	}

	/**
	 * Gets the request.
	 *
	 * @return the request
	 */
	public FilterData getRequest() {
		return request;
	}

	/**
	 * Gets the bson filter.
	 *
	 * @return the bson filter
	 */
	public Bson getBsonFilter() {
		return bsonFilter;
	}

	public OperationContext getContext() {
		return operationContext;
	}
	
	/**
	 * Gets the bsonprojection.
	 *
	 * @return the bsonprojection
	 */
	public Bson getBsonprojection() {
		return bsonprojection;
	}

	/**
	 * Gets the sort spec.
	 *
	 * @return the sort spec
	 */
	public Bson getSortSpec() {
		return sortSpec;
	}

}
