//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.GetRequest;
import com.boomi.util.json.JsonPayloadUtil;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.util.BaseGetOperation;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.logging.Level;


/**
 * Implementation of {@link BaseGetOperation} to return the bucket details for
 * the given bucket Id.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class GetOperation extends BaseGetOperation {

	private static final String NOT_FOUND = "404";
	private static final String ERROR_EMPTY_ID = "the ID parameter is empty or only contains blank spaces";
	private static final String ERROR_NO_RESPONSE = "No response";

	/**
	 * Creates a new {@link GetOperation} instance
	 *
	 * @param connection an PrismConnection instance
	 */
	public GetOperation(PrismOperationConnection connection) {
		super(connection);
	}

	@Override
	protected void executeGet(GetRequest request, OperationResponse opResponse) {
		ObjectIdData data = request.getObjectId();
		PrismResponse response = null;
		String bucketId = data.getObjectId();
		if (StringUtil.isBlank(bucketId)) {
			opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, ConnectorException.NO_CODE, ERROR_EMPTY_ID,
					null);
			return;
		}
		try {
			response = getConnection().getBucket(bucketId);
			handleResponse(opResponse, data, response);
		} catch (ConnectorException e) {
			data.getLogger().log(Level.WARNING, e.getMessage(), e);
			opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), e.getStatusMessage(),
					null);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(opResponse, data, e);
		} finally {
			IOUtil.closeQuietly(response);
		}
	}

	/**
	 * Adds prism response to the output payload
	 * 
	 * @param opResponse an OperationResponse instance
	 * @param data       an ObjectIdData instance
	 * @param response   a PrismResponse instance
	 */
	private void handleResponse(OperationResponse opResponse, ObjectIdData data, PrismResponse response) {
		 
		Payload payload=null;
		if (response.isNotFound()) {
			
			try {
				JsonNode json = response.getJsonEntity();
				payload = JsonPayloadUtil.toPayload(json);
				ResponseUtil.addApplicationError(opResponse, data, NOT_FOUND, payload);
			} catch (IOException e) {
				throw new ConnectorException(ERROR_NO_RESPONSE);
			}finally {
				IOUtil.closeQuietly(payload);
			}

		} else {
			response.addResult(data, opResponse);
		}
	}

	@Override
	public PrismOperationConnection getConnection() {
		return (PrismOperationConnection) super.getConnection();
	}
}
