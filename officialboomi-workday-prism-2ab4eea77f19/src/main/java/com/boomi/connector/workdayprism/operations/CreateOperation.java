//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.connector.workdayprism.PrismConnection;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.connector.workdayprism.utils.CreateBucketHelper;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;

/**
 * Implementation of {@link SizeLimitedUpdateOperation} to give support to create
 * buckets or datasets based on the selected object type Id during browsing.
 *
 * @author saurav.b.sengupta <saurav.b.sengupta@accenture.com>
 */
public class CreateOperation extends SizeLimitedUpdateOperation {
	private final String objectTypeId;

	/**
	 * Creates a new {@link CreateOperation} instance
	 *
	 * @param connection a {@link PrismConnection} instance
	 */
	public CreateOperation(PrismOperationConnection connection) {
		super(connection);
		this.objectTypeId = connection.getOperationContext().getObjectTypeId();
	}

	@Override
	public PrismOperationConnection getConnection() {
		return (PrismOperationConnection) super.getConnection();
	}

	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		this.executeCreateOperation(request, response);
	}

	/**
	 * Custom method to perform create operation
	 * 
	 * @param opRequest  an instance of UpdateRequest
	 * @param opResponse an instance of OperationResponse
	 */
	protected void executeCreateOperation(UpdateRequest opRequest, OperationResponse opResponse) {
		for (ObjectData data : opRequest) {
			PrismResponse response = null;
			try {
				response = Constants.ENTITY_DATASET.equals(objectTypeId) ? createDataset(data) : createBucket(data);
				response.addResult(data, opResponse);
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
	}

	/**
	 * Invokes getConnection() method under PrismConnection class to create a table
	 * 
	 * @param input of type ObjectData
	 * @return PrismResponse instance
	 * @throws IOException
	 */
	public PrismResponse createDataset(ObjectData input) throws IOException {
		return getConnection().createTable(input);
	}

	/**
	 * Invokes getConnection() method under PrismConnection class to create a bucket
	 * 
	 * @param input of type ObjectData
	 * @return PrismResponse instance
	 * @throws IOException
	 */
	public PrismResponse createBucket(ObjectData input) throws IOException {
		JsonNode fields = getConnection().getSelectedTableSchema();
		PropertyMap properties = getContext().getOperationProperties();
		JsonNode schema = null;
		JsonNode schemaTemp = null;
		InputStream inputStream = null;
		JsonNode node;
		try {
			long inputDataSize = input.getDataSize();
			inputStream = input.getData();
			if (inputDataSize != 0) {
				schemaTemp = JSONUtil.parseNode(inputStream);
				schema = schemaTemp.asText().equals("Role") ? CreateBucketHelper.buildSchemaIfInputIsNull()
						: schemaTemp;
			} else {
				schema = CreateBucketHelper.buildSchemaIfInputIsNull();
			}

			node = CreateBucketHelper.buildBucketPayload(properties, objectTypeId, schema, fields);

		} catch (IOException e) {
			throw new ConnectorException(Constants.ERROR_WRONG_INPUT_PROFILE, e);
		} finally {
			IOUtil.closeQuietly(inputStream);
		}

		return getConnection().createBucket(node);
	}
}
