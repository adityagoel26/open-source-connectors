//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import java.util.logging.Level;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.model.UploadResponse;
import com.boomi.connector.workdayprism.operations.upload.UploadHelper;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.databind.JsonNode;

/** Class to execute the IMPORT operation for version 2.0 of Workday API
 * @author saurav.b.sengupta
 *
 */
public class ImportOperation extends SizeLimitedUpdateOperation {
    	
	/**
     * Creates a new ImportOperation instance
     *
     * @param connection
     *         an PrismConnection instance
     */
	private String bucketId;
	public ImportOperation(PrismOperationConnection connection) {
		super(connection);
	}
	
	@Override
	protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		this.executeImportOperation(request, response);	
	}

	/** Custom method to perform import operation
	 * @param request an instance of UpdateRequest
	 * @param opResponse an instance of OperationResponse
	 */
	protected void executeImportOperation(UpdateRequest request, OperationResponse opResponse) {
		CreateOperation createOperation=new CreateOperation(getConnection()); 
		for (ObjectData data : request) { 
		  PrismResponse response = null;
		  UploadResponse uploadResponse=null;
		  CompleteBucketOperation completeBucketOperation=null;
		  try {
			  response=createOperation.createBucket(data);
			  JsonNode bucketJson=response.getJsonEntity();
			  bucketId=bucketJson.findPath(Constants.FIELD_BUCKET_ID).asText();
			  if(response.isSuccess()) {
				  UploadHelper uploadHelper = new UploadHelper(getConnection());
				  uploadResponse=uploadHelper.upload(data, bucketId);
				  if(uploadResponse.isSuccess()) {
					  completeBucketOperation=new CompleteBucketOperation(getConnection());
					  completeBucketOperation.processInput(data, opResponse, bucketId);
				  }
			  }
		} catch (ConnectorException e) {
			data.getLogger().log(Level.WARNING, e.getMessage(), e);
			opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), e.getStatusMessage(),
					null);
		} catch (Exception e) {
			ResponseUtil.addExceptionFailure(opResponse, data, e);
		} finally {
			IOUtil.closeQuietly(response);
			IOUtil.closeQuietly(uploadResponse);
		}
		}	
	}
	

	public String getBucketId() {
		return bucketId;
	}

	@Override
    public PrismOperationConnection getConnection() {
        return (PrismOperationConnection) super.getConnection();
	}
}
