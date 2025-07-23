// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.hubspotcrm.HubspotcrmConnection;
import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmOperationType;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HttpClientFactory;
import com.boomi.connector.hubspotcrm.util.ResponseHandler;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.Args;
import com.boomi.util.IOUtil;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Represents an update operation for the Hubspot CRM connector.
 * This class extends OpenAPIOperation to provide specific functionality
 * for updating records in Hubspot CRM.
 *
 * <p>This class likely handles the process of updating existing records
 * in Hubspot CRM using the OpenAPI specification.</p>
 */
public class HubspotcrmUpdateOperation extends BaseUpdateOperation {

    private static final String ID = "id";
    private static final String ID_TYPE = "idType";
    private static final String ID_TYPE_EMAIL = "?idProperty=email";
    private static final String OBJECT_ID_EMPTY_MESSAGE = "Object ID cannot be null or empty";

    private static final String OPERATION_TYPE = "update";

    public HubspotcrmUpdateOperation(HubspotcrmOperationConnection connection) {
        super(connection);
    }

    /**
     * Overrides the getPath method to generate the appropriate path for the update operation.
     *
     * @param data The ObjectData containing the data for the operation.
     * @return The path for the update operation.
     */

    protected String getPath(ObjectData data) {
        String idType = data.getDynamicOperationProperties().getProperty(ID_TYPE);
        String objectId = getObjectId(data);

        return (idType == null || idType.equalsIgnoreCase(ID)) ? objectId : (objectId + ID_TYPE_EMAIL);
    }

    /**
     * Retrieves the object ID from the ObjectData.
     *
     * @param data The ObjectData containing the data for the operation.
     * @return The objectId as a string.
     */
    private String getObjectId(ObjectData data) {
        String objectId = data.getDynamicOperationProperties().getProperty(ID);
        Args.notBlank(objectId, OBJECT_ID_EMPTY_MESSAGE);
        return getContext().getObjectTypeId().split("::")[1].split("\\{")[0] + objectId;
    }

    /**
     * Executes an update operation by processing each record in the request sequentially.
     *
     * @param request  The UpdateRequest containing the collection of ObjectData records to be processed.
     *                 Each ObjectData represents a single record that needs to be updated.
     * @param response The OperationResponse object that will contain the results of the operation.
     *                 Success or failure status for each processed record will be added to this response.
     *                 <p>
     *                 The method:
     *                 1. Iterates through each ObjectData in the request
     *                 2. Processes each record individually using processRecord method
     *                 3. Accumulates the results in the provided response object
     */
    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        try (CloseableHttpClient client = HttpClientFactory.createHttpClient()) {
            for (ObjectData data : request) {
                processRecord(data, response, client);
            }
        } catch (IOException e) {
            response.getLogger().log(Level.SEVERE, () -> "Failed executing search operation: " + e.getMessage());
            throw new ConnectorException("IOException occurred while executing update operation: {0}", e);
        }
    }

    /**
     * Processes a single record by making an HTTP request to the HubSpot CRM API.
     *
     * @param data              The ObjectData containing:
     *                          - The record data to be processed
     *                          - Associated metadata
     *                          - Logging capabilities
     * @param operationResponse The response object that will contain:
     *                          - Success/failure status
     *                          - Error messages if any
     *                          - Response payload
     *                          - Operation status codes
     * @see HubspotcrmConnection For API connection details
     * @see ExecutionUtils#execute For HTTP execution implementation
     * @see IOUtil#closeQuietly For resource cleanup
     */
    private void processRecord(ObjectData data, OperationResponse operationResponse, CloseableHttpClient client)
            throws IOException {
        CloseableHttpResponse response = null;
        try {
            List<Map.Entry<String, String>> headers =
                    ((HubspotcrmOperationConnection) this.getConnection()).getHeaders();
            String path = getPath(data);
            String operationType = HubspotcrmOperationType.UPDATE.getOperationTypeId();
            response = ExecutionUtils.execute(operationType, path, client, data.getData(),
                    ((HubspotcrmOperationConnection) this.getConnection()).getUrl(), headers, data.getLogger());
            ResponseHandler.handleResponse(operationResponse, data, response, HttpStatus.SC_OK, OPERATION_TYPE);
        } catch (IOException e) {
            ResponseHandler.handleError(operationResponse, data, e, OPERATION_TYPE);
        } finally {
            if (response != null) {
                IOUtil.closeQuietly(response);
            }
        }
    }
}