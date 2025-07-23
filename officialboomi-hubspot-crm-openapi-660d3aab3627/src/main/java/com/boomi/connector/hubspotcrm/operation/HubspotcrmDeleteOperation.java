// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HttpClientFactory;
import com.boomi.connector.util.BaseDeleteOperation;
import com.boomi.util.Args;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Represents the Hubspotcrm Archive Operation.
 * This class is responsible for handling the archive of objects in the Hubspotcrm API. It extends the
 * {@link BaseDeleteOperation} to leverage the archive functionality and customizes it to perform
 * archive operations on the Hubspotcrm API.
 */
public class HubspotcrmDeleteOperation extends BaseDeleteOperation {

    private static final String EMPTY_OBJECT_ID_MSG = "ID is required for archive operation";

    public HubspotcrmDeleteOperation(HubspotcrmOperationConnection connection) {
        super(connection);
    }

    /**
     * Executes the archive operation for the given DeleteRequest.
     *
     * @param deleteRequest     The DeleteRequest containing the object IDs to be deleted.
     * @param operationResponse The OperationResponse object to store the response of the archive operation.
     */
    @Override
    protected void executeDelete(DeleteRequest deleteRequest, OperationResponse operationResponse) {
        try (CloseableHttpClient client = HttpClientFactory.createHttpClient()) {
            List<Map.Entry<String, String>> headers =
                    ((HubspotcrmOperationConnection) this.getConnection()).getHeaders();
            String operationType = this.getContext().getOperationType().name();
            for (ObjectIdData data : deleteRequest) {
                if (!validateObjectID(data, operationResponse)) {
                    continue;
                }
                /**
                 * Constructs the complete API path by combining the base path with the object ID.
                 *
                 * @param data Contains the object ID of the record to be processed
                 * @return String The complete path for the API request
                 *
                 * Example:
                 * If getPath() returns "/crm/v3/objects/contacts/"
                 * and data.getObjectId() returns "123"
                 * Then the result will be "/crm/v3/objects/contacts/123"
                 */
                String path = getPath() + data.getObjectId();
                archiveRecord(operationType, path, client, ((HubspotcrmOperationConnection) this.getConnection()),
                        headers, operationResponse, data);
            }
        } catch (IOException e) {
            throw new ConnectorException("Failed executing archive operation", e);
        }
    }

    /**
     * Validates the object ID from the provided ObjectIdData.
     * This method checks if the object ID is not blank. If the object ID is valid,
     * it returns true. If the object ID is blank, it adds an exception failure to
     * the operation response and returns false.
     *
     * @param data              The ObjectIdData containing the object ID to validate.
     * @param operationResponse The OperationResponse to update in case of validation failure.
     * @return true if the object ID is valid (not blank), false otherwise.
     * @throws IllegalArgumentException if the object ID is blank (caught internally)
     */
    private static boolean validateObjectID(ObjectIdData data, OperationResponse operationResponse) {
        try {
            Args.notBlank(data.getObjectId(), EMPTY_OBJECT_ID_MSG);
            return true;
        } catch (IllegalArgumentException exception) {
            ResponseUtil.addExceptionFailure(operationResponse, data, exception);
            return false;
        }
    }

    /**
     * Executes a archive operation for object in the Hubspot CRM.
     *
     * @param operationType                 The type of operation being performed (e.g., "Archive").
     * @param path                          The API endpoint path for the archive operation.
     * @param client                        The HTTP client used to send the request.
     * @param hubspotcrmOperationConnection The connection object for Hubspot CRM.
     * @param headers                       The HTTP headers to be included in the request.
     * @param operationResponse             The response object to be populated with the operation result.
     * @param data                          The object ID data for the item to be deleted.
     *                                      <p>
     *                                      This method attempts to archive object from Hubspot CRM. It executes the
     *                                      HTTP
     *                                      request,
     *                                      extracts the response, and handles any IOExceptions that may occur during
     *                                      the
     *                                      process.
     *                                      If an IOException occurs, it logs the error and adds an exception failure
     *                                      to the
     *                                      operation response.
     */
    private static void archiveRecord(String operationType, String path, CloseableHttpClient client,
            HubspotcrmOperationConnection hubspotcrmOperationConnection, List<Map.Entry<String, String>> headers,
            OperationResponse operationResponse, ObjectIdData data) {
        try (CloseableHttpResponse response = ExecutionUtils.execute(operationType, path, client, null,
                hubspotcrmOperationConnection.getUrl(), headers, data.getLogger())) {
            extractResponse(operationResponse, data, response);
        } catch (IOException e) {
            data.getLogger().log(Level.SEVERE, "IOException occurred while executing archive operation: {0}",
                    e.getMessage());
            ResponseUtil.addExceptionFailure(operationResponse, data, e);
        }
    }

    /**
     * Extracts the response from the given CloseableHttpResponse and adds the result or error result
     * to the OperationResponse object based on the HTTP status code.
     *
     * @param operationResponse The OperationResponse object to add the result or error result to.
     * @param data              The ObjectIdData object containing data related to the operation.
     * @param response          The CloseableHttpResponse object containing the response from the HTTP request.
     */
    private static void extractResponse(OperationResponse operationResponse, ObjectIdData data,
            CloseableHttpResponse response) {
        if (HttpStatus.SC_NO_CONTENT == response.getStatusLine().getStatusCode()) {
            operationResponse.addResult(data, OperationStatus.SUCCESS, response.getStatusLine().getReasonPhrase(), "",
                    null);
        } else {
            operationResponse.addErrorResult(data, OperationStatus.APPLICATION_ERROR,
                    String.valueOf(response.getStatusLine().getStatusCode()),
                    response.getStatusLine().getReasonPhrase(), null);
        }
    }

    /**
     * Extracts the base API path from the object type ID by performing string manipulation.
     *
     * @return String The base path segment for the API endpoint
     * Example:
     * If getContext().getObjectTypeId() returns "DELETE::/crm/v3/objects/contacts/{contactId}"
     * Then:
     * 1. split("::")[1] extracts "crm/v3/objects/contacts/{contactId}"
     * 2. split("\\{")[0] extracts "crm/v3/objects/contacts/"
     * Final result: "crm/v3/objects/contacts/"
     */
    private String getPath() {
        return getContext().getObjectTypeId().split("::")[1].split("\\{")[0];
    }
}
