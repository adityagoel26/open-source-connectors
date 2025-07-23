//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.json.JsonPayloadUtil;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.operations.status.GetStatusHelper;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;

/**
 * Implementation of {@link BaseUpdateOperation} to give support to the â€œComplete Bucketâ€� endpoint in Workday Prism.
 * It should be invoke once all the needed files have been uploaded into a bucket and its data is ready to be processed
 * and persisted into the dataset.
 *
 * It provides the option to wait until the service finishes processing the bucket and return the final state using the
 * "Wait For Completion" operation property.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class CompleteBucketOperation extends BaseUpdateOperation {
    private static final String BUCKET_ID = "id";
    private static final String ERROR_MISSING_BUCKET_ID_NODE = "missing " + BUCKET_ID + " node.";
    private static final String PROPERTY_WAIT_FOR_COMPLETION = "wait_for_completion";
    private static final String PROPERTY_TIMEOUT = "timeout";
    private static final String FAILED = "Failed";

    private final boolean waitForCompletion;
    private final Long statusTimeout;

    /**
     * Creates a new CompleteBucketOperation instance
     *
     * @param connection
     *         an PrismConnection instance
     */
    public CompleteBucketOperation(PrismOperationConnection connection) {
        super(connection);
        this.waitForCompletion = connection.getBooleanProperty(PROPERTY_WAIT_FOR_COMPLETION);
        this.statusTimeout = connection.getLongProperty(PROPERTY_TIMEOUT, 0L);
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse opResponse) {
        for (ObjectData data : request) {
            processInput(data, opResponse, null);
        }
    }

    /** Method for adding the success payload or failure payload into ResponseUtil methods based on the state
     * @param data
     * @param opResponse
     * @param response
     * @throws IOException
     */
    private static void handleResponse(ObjectData data, OperationResponse opResponse, PrismResponse response)
            throws IOException {
        JsonNode json = response.getJsonEntity();
        String status = json.path(Constants.FIELD_STATE).path(Constants.FIELD_DESCRIPTOR).asText();
        if (FAILED.equalsIgnoreCase(status)) {
            Payload payload = JsonPayloadUtil.toPayload(json);
            ResponseUtil.addApplicationError(opResponse, data, String.valueOf(response.getStatusCode()), payload);
        }
        else {
            response.addResult(data, opResponse);
        }
    }
   
    /** This method helps in completing the bucket and upload data from file to table 
     * @param data an instance of ObjectData 
     * @param opResponse an instance OperationResponse
     */
    public void processInput(ObjectData data, OperationResponse opResponse, String bucketIdSet) {
        PrismResponse response = null;
        try {
            String bucketId = bucketIdSet==null?extractBucketId(data):bucketIdSet;
            response = getConnection().completeBucket(bucketId);

            if (response.isSuccess() && waitForCompletion) {
                IOUtil.closeQuietly(response);
                response = new GetStatusHelper(getConnection(), statusTimeout).getStatus(bucketId);

                handleResponse(data, opResponse, response);
            } else {
                response.addResult(data, opResponse);
            }
        }
        catch (ConnectorException e) {
            data.getLogger().log(Level.WARNING, e.getMessage(), e);
            opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), e.getMessage(), null);
        }
        catch (Exception e) {
            ResponseUtil.addExceptionFailure(opResponse, data, e);
        }
        finally {
            IOUtil.closeQuietly(response);
        }
    }

    /** Returns the bucket id fetched from the request payload profile 
     * @param data
     * @return String
     */
    private static String extractBucketId(ObjectData data) {
        InputStream input = null;
        try {
            input = data.getData();
            JsonNode bucketIdNodetEST = JSONUtil.parseNode(input);
            JsonNode bucketIdNode = bucketIdNodetEST.findPath(BUCKET_ID);
            if (bucketIdNode.isMissingNode()) {
                throw new ConnectorException(ERROR_MISSING_BUCKET_ID_NODE);
            }
            return bucketIdNode.asText();
        }
        catch (IOException e) {
            throw new ConnectorException(Constants.ERROR_WRONG_INPUT_PROFILE, e);
        }
        finally {
            IOUtil.closeQuietly(input);
        }
    }

    @Override
    public PrismOperationConnection getConnection() {
        return (PrismOperationConnection) super.getConnection();
    }
}
