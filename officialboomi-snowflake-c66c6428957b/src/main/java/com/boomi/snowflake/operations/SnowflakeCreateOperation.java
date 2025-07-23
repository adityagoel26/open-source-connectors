// Copyright (c) 2025 Boomi, LP
package com.boomi.snowflake.operations;

import java.io.InputStream;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.logging.Level;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.controllers.SnowflakeCreateController;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.snowflake.util.BoundedMap;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.util.TableDefaultAndMetaDataObject;
import com.boomi.util.IOUtil;

/**
 * The Class SnowflakeCreateOperation.
 *
 * @author Vanangudi,S
 */
public class SnowflakeCreateOperation extends SizeLimitedUpdateOperation {

	/** The Constant APPLICATION_ERROR_MESSAGE. */
	private static final String APPLICATION_ERROR_MESSAGE = "Error in batch %d: ";
	/** The Constant TWO. */
	private static final int TWO = 2;
	private SortedMap<String, String> _metaData;
	private BoundedMap<String, TableDefaultAndMetaDataObject> _boundedMap;

	/**
	 * Instantiates a new Snowflake Create Operation
	 * @param conn Snowflake connection parameters
	 */
	@SuppressWarnings("unchecked")
	public SnowflakeCreateOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * This function is called when CREATE operation gets called
	 * @param request
	 * 			Update Request
	 * @param response
	 * 			Update Response
	 */
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		ConnectionProperties properties = null;
		SnowflakeCreateController controller = null;
		try {
			PropertyMap operationProperties=getContext().getOperationProperties();
			properties = new ConnectionProperties(getConnection(), operationProperties,
					getContext().getObjectTypeId(), response.getLogger());
			controller = new SnowflakeCreateController(properties);
			Iterator<ObjectData> requestDataIterator = request.iterator();
			String tableName = properties.getTableName();
			if (tableName != null && tableName.lastIndexOf("\".\"") != -1) {
				tableName = tableName.substring(tableName.lastIndexOf("\".\"")+TWO);
			}
			String cookieValue = getCookie();
			_boundedMap = SnowflakeOperationUtil.getBoundedMap(_boundedMap);
			_metaData = SnowflakeOperationUtil.processCookieAndMetadata(operationProperties, properties,
					cookieValue, _boundedMap);
			executeUpdate(response, properties, controller, requestDataIterator, tableName);
		} catch (Exception e) {
			throw new ConnectorException(e);
		} finally {
			/*
			 * finalizes closes controller if it's not null
			 * otherwise commit and close properties
			 */
			if (controller == null) {
				if (properties != null) {
					properties.commitAndClose();
				}
			} else {
				controller.closeResources();
			}
		}
	}

	private void executeUpdate(OperationResponse response, ConnectionProperties properties,
			SnowflakeCreateController controller, Iterator<ObjectData> requestDataIterator,
			String tableName) throws SQLException {
		boolean lastRequestData;
		while (requestDataIterator.hasNext()) {
			ObjectData requestData = requestDataIterator.next();
			lastRequestData = !requestDataIterator.hasNext();
			requestData.getLogger().info("Parsing document");
			InputStream inputData = requestData.getData();
			InputStream result = null;
			/**
			 * Fetches metadata using database and schema from the first document's dynamic operation properties.
			 * Metadata is only retrieved if:
			 * - Current metadata is null (_metaData == null)
			 * - Table name is provided (tableName != null)
			 *
			 */
			DynamicPropertyMap dynamicProperties = requestData.getDynamicOperationProperties();
			if (_metaData == null && tableName != null) {
				_metaData = SnowflakeOperationUtil.getMetadataValues(
						properties, tableName,
						dynamicProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
						dynamicProperties.getProperty(SnowflakeOverrideConstants.SCHEMA));
			}
			try {
                controller.receive(JSONHandler.readSortedMap(inputData), properties.getEmptyValueInput(), _metaData,
						_boundedMap, properties.getTableName(), requestData.getDynamicOperationProperties());
				if (lastRequestData) {
					controller.executeLastBatch();
				}

				if (properties.getReturnResults()) {
					result = controller.getResultFromStatement(lastRequestData);
					if (result != null) {
						ResponseUtil.addSuccess(response, requestData, "0", ResponseUtil.toPayload(result));
					} else {
						ResponseUtil.addEmptySuccess(response, requestData, null);
					}
				} else {
					ResponseUtil.addSuccess(response, requestData, "0", ResponseUtil.toPayload(inputData));
				}
			} catch (ConnectorException e) {
				String errorMessage =
						String.format(APPLICATION_ERROR_MESSAGE, controller.getCurrentBatch()) + e.getMessage();
				requestData.getLogger().log(Level.WARNING, errorMessage, e);
				response.addResult(requestData, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), errorMessage,
						ResponseUtil.toPayload(errorMessage));
			} catch (Exception e) {
				SnowflakeOperationUtil.handleGeneralException(response, requestData, e);
			} finally {
				IOUtil.closeQuietly(inputData);
				IOUtil.closeQuietly(result);
			}
		}
	}

	/**
	 * Gets the connection.
	 *
	 * @return the Snowflake connection
	 */
	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}

	/**
	 * Retrieves the cookie for the input object definition role.
	 *
	 * @return The cookie as a {@code String}. Returns {@code null} if not found.
	 */
	private String getCookie(){
		return getContext().getObjectDefinitionCookie(ObjectDefinitionRole.INPUT);
	}


}