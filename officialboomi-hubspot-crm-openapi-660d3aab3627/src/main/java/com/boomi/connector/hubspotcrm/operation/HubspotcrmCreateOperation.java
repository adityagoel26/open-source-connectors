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
import com.boomi.util.IOUtil;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Represents the Hubspotcrm Create Operation.
 * <p>
 * This class extends the OpenAPIOperation to provide specific functionality
 * for creating records in HubSpot CRM. It implements the necessary methods
 * to handle the creation process according to HubSpot's API specifications.
 *
 * <p>Usage of this class typically involves instantiating it and calling
 * its methods to perform create operations on HubSpot CRM objects.</p>
 */
public class HubspotcrmCreateOperation extends BaseUpdateOperation {

    private static final String OPERATION_TYPE = "create";

    public HubspotcrmCreateOperation(HubspotcrmOperationConnection connection) {
        super(connection);
    }

    /**
     * Extracts the last segment of a path from the connection configuration.
     * <p>
     * The ObjectData containing operation data (not used in current implementation)
     *
     * @return String The last segment of the path after splitting by "::"
     * <p>
     * Example:
     * If connection path is "POST::/crm/v3/objects/contacts"
     * Returns "/crm/v3/objects/contacts"
     * <p>
     * If connection path is "POST::/crm/v3/objects/companies"
     * Returns "/crm/v3/objects/companies"
     */
    private String getPath() {
        return getContext().getObjectTypeId().split("::")[1];
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
            throw new ConnectorException("IOException occurred while executing create operation: {0}", e);
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
    private void processRecord(ObjectData data, OperationResponse operationResponse, CloseableHttpClient client) {
        CloseableHttpResponse response = null;
        try {
            List<Map.Entry<String, String>> headers =
                    ((HubspotcrmOperationConnection) this.getConnection()).getHeaders();
            String path = getPath();
            String operationType = HubspotcrmOperationType.CREATE.getOperationTypeId();
            response = ExecutionUtils.execute(operationType, path, client, data.getData(),
                    ((HubspotcrmOperationConnection) this.getConnection()).getUrl(), headers, data.getLogger());
            ResponseHandler.handleResponse(operationResponse, data, response, HttpStatus.SC_CREATED, OPERATION_TYPE);
        } catch (IOException e) {
            ResponseHandler.handleError(operationResponse, data, e, OPERATION_TYPE);
        } finally {
            if (response != null) {
                IOUtil.closeQuietly(response);
            }
        }
    }
}