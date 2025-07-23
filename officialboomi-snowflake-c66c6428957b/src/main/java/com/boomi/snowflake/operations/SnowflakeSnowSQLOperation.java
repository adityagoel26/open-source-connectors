// Copyright (c) 2021 Boomi, Inc.
package com.boomi.snowflake.operations;

import java.io.InputStream;
import java.util.Iterator;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.boomi.util.IOUtil;

/**
 * The SnowflakeSnowSQLOperation class
 * @author s.vanangudi
 *
 */
public class SnowflakeSnowSQLOperation extends SizeLimitedUpdateOperation {

	/**
	 * Instantiates a SnowflakeSnowSQLOperation.
	 * @param conn
	 * 			the Snowflake Connection
	 * */
	@SuppressWarnings("unchecked")
	public SnowflakeSnowSQLOperation(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * this function is called when SnowSQL operation is done
	 * @param request Update Request
	 * @param response Operation Response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		ConnectionProperties properties = null;
		SnowflakeWrapper wrapper = null;
		try {
			properties = new ConnectionProperties(getConnection(), getContext().getOperationProperties(),
					getContext().getObjectTypeId(), response.getLogger());
			wrapper = new SnowflakeWrapper(properties.getConnectionGetter(), properties.getConnectionTimeFormat(),
					properties.getLogger(), properties.getTableName());
			wrapper.setAutoCommit(false);
			Iterator<ObjectData> requestDataIterator = request.iterator();
			if (requestDataIterator.hasNext()) {
				boolean lastRequestData;
				do {
					ObjectData requestData = requestDataIterator.next();
					lastRequestData = !requestDataIterator.hasNext();
					executeUpdate(response, properties, wrapper, lastRequestData, requestData);
				} while (!lastRequestData);
			}
		} catch(ClassCastException e) {
			throw new ConnectorException("Invalid input data format. Please provide the input in JSON format, Reason: "
					+ e.getLocalizedMessage(), e);
		} catch (Exception e) {
			throw new ConnectorException("Error during operation, Reason: " + e.getLocalizedMessage(), e);
		} finally {
			if (wrapper != null) {
				wrapper.close();
			} else {
				if (properties != null) {
					properties.commitAndClose();
				}
			}
		}
	}

	private void executeUpdate(OperationResponse response, ConnectionProperties properties, SnowflakeWrapper wrapper,
			boolean lastRequestData, ObjectData requestData) {
		InputStream result = null;
		Long batchSize = properties.getBatchSize();
		Long numberOfScripts = properties.getNumberOfScripts();
		try {
			String snowSQLScript = properties.getSnowflakeCommand().getSQLString(requestData);
			if (numberOfScripts < 0) {
				throw new ConnectorException("The Number of SnowSQL Statements must be a positive integer.");
			}
			if (properties.getNumberOfNonZeroScripts() != null
					&& properties.getNumberOfNonZeroScripts() == 0) {
				throw new ConnectorException("The Number of SnowSQL Statements cannot be 0.");
			}
			if (batchSize == 1 && properties.getReturnResults()) {
				wrapper.executeMultiStatement(snowSQLScript, numberOfScripts, null);
				while ((result = wrapper.getResultFromNextStatement()) != null) {
					ResponseUtil.addPartialSuccess(response, requestData, "0", ResponseUtil.toPayload(result));
				}
				response.finishPartialResult(requestData);
			} else {
				wrapper.executeMultiStatementBatch(snowSQLScript, batchSize, lastRequestData, numberOfScripts);
				ResponseUtil.addEmptySuccess(response, requestData, "0");
			}
		} catch (ConnectorException e) {
			SnowflakeOperationUtil.handleConnectorException(response, requestData, e);
		} catch (Exception e) {
			SnowflakeOperationUtil.handleGeneralException(response, requestData, e);
		} finally {
			IOUtil.closeQuietly(result);
		}
	}

	/**
	 * gets the connection
	 */
	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}
}
