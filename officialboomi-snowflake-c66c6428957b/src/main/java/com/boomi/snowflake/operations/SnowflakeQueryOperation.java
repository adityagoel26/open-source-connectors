// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.QueryHandler;
import com.boomi.snowflake.wrappers.SnowflakeTableStream;
import com.boomi.util.IOUtil;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.logging.Level;

public class SnowflakeQueryOperation extends BaseQueryOperation {

	/** The Constant SNOWFLAKE_BATCHING. */
	private static final String SNOWFLAKE_BATCHING = "documentBatching";

	public SnowflakeQueryOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * This function is called when QUERY operation is called
	 */
	@Override
	protected void executeQuery(QueryRequest request, OperationResponse response) {
		response.getLogger().entering(this.getClass().getCanonicalName(), "executeQuery()");
		request.getFilter().getLogger().info("Started processing");
		ConnectionProperties properties = null;
		SnowflakeTableStream reader = null;
		try {
			PropertyMap operationProperties = getContext().getOperationProperties();
			String cookie = getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
			boolean isBatching = false;
			if(cookie != null && !cookie.isEmpty()) {
				SortedMap<String, String> cookieMap = JSONHandler.readSortedMap(cookie);
				if(cookieMap != null && !cookieMap.isEmpty()) {
					operationProperties.putIfAbsent(SNOWFLAKE_BATCHING, Boolean.valueOf(cookieMap.get(SNOWFLAKE_BATCHING)));
					isBatching = Boolean.valueOf(cookieMap.get(SNOWFLAKE_BATCHING));
				}
			}
			properties = new ConnectionProperties(getConnection(), operationProperties,
					getContext().getObjectTypeId(), response.getLogger());
			
			List<String> selectedFields = getContext().getSelectedFields();
			List<String> selectedFieldsWithoutArray = new ArrayList<>();
			Iterator<String> fields = selectedFields.iterator();  
			if (isBatching) {
				while (fields.hasNext()) {
					selectedFieldsWithoutArray.add(String.valueOf(fields.next()).substring(6));
				}
			} else {
				selectedFieldsWithoutArray = selectedFields;
			}
			QueryFilter filter = request.getFilter().getFilter();
			List<Map<String, String>> expressionList = QueryHandler.getExpression(filter.getExpression(), isBatching);
			List<String> orderItems = QueryHandler.getOrderItems(filter.getSort(), isBatching);

			reader = readFromStream(request, response, properties, selectedFieldsWithoutArray, expressionList, orderItems);
		} catch (ConnectorException e) {
			request.getFilter().getLogger().log(Level.WARNING, e.getMessage(), e);
			response.addResult(request.getFilter(), OperationStatus.APPLICATION_ERROR, e.getStatusCode(),
					e.getMessage(), ResponseUtil.toPayload(e.getMessage()));
		} catch (Exception e) {
			request.getFilter().getLogger().log(Level.SEVERE, e.getMessage());
			ResponseUtil.addExceptionFailure(response, request.getFilter(), e);
		} finally {
			/*
			 * closes reader if it's not null
			 * otherwise commit and close properties
			 */
			if (reader != null) {
				reader.closeReader();
			} else {
				if (properties != null) {
					properties.commitAndClose();
				}
			}
		}
	}

	/**
	 * Reads data from a Snowflake table stream and sends each row as a separate file in the response.
	 *
	 * @param request the QueryRequest object containing the request details
	 * @param response the OperationResponse object to which the results will be added
	 * @param properties the ConnectionProperties object containing the connection properties
	 * @param fields a list of field names to be included in the result
	 * @param expression a list of expressions to be applied to the result
	 * @param orderItems a list of items to be used for ordering the result
	 * @return the SnowflakeTableStream object used for reading the data
	 */
	private SnowflakeTableStream readFromStream(QueryRequest request, OperationResponse response, ConnectionProperties properties,
			List<String> fields, List<Map<String, String>> expression, List<String> orderItems) {
		SnowflakeTableStream reader = new SnowflakeTableStream(properties , fields, null, orderItems, expression
				,request.getFilter().getDynamicOperationProperties());
		if (reader.next()) {
			// get table and send each row in a separate file
			do {
				InputStream batch = reader.getBatchedRow();
				response.addPartialResult(request.getFilter(), OperationStatus.SUCCESS, "200", "0", ResponseUtil.toPayload(batch));
				IOUtil.closeQuietly(batch);
			} while (reader.next());
			response.finishPartialResult(request.getFilter());
		} else {
			ResponseUtil.addEmptySuccess(response, request.getFilter(), "0");
		}
		return reader;
	}

	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}
