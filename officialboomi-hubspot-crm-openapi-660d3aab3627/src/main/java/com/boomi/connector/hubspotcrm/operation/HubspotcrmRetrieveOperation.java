// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.util.ConstantAssociation;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HttpClientFactory;
import com.boomi.connector.util.BaseGetOperation;
import com.boomi.util.Args;
import com.boomi.util.StringUtil;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * This class represents the Retrieve operation for the HubSpot CRM connector.
 * It extends the BaseGetOperation class and overrides the executeGet method to handle the Retrieve request.
 */
public class HubspotcrmRetrieveOperation extends BaseGetOperation {

    private static final String EMAIL_ID = "emailId";
    private static final String ID_TYPE = "idType";
    private static final String EMPTY_OBJECT_ID = "ID is required for retrieve operation";
    private static final String ID_TYPE_EMAIL = "?idProperty=email";
    private static final String URL_PROPERTIES_KEY = "properties=";
    private static final String URL_ASSOCIATIONS_KEY = "associations=";
    private static final String QUERY_SEPARATOR = "?";
    private static final String AMPERSAND_SEPARATOR = "&";
    private static final String PERCENTAGE = "%";

    /**
     * Constructs a new HubSpot CRM retrieve operation instance.
     * Initializes the operation with the provided connection settings by calling the parent constructor.
     *
     * @param connection The HubSpot CRM connection configuration containing authentication and endpoint details
     * @see HubspotcrmOperationConnection
     */
    public HubspotcrmRetrieveOperation(HubspotcrmOperationConnection connection) {
        super(connection);
    }

    /**
     * Executes a retrieve operation for the HubSpot CRM connector.
     *
     * @param getRequest        The GET request containing the object ID to retrieve.
     * @param operationResponse The response object to which the operation result will be added.
     * @throws ConnectorException If an error occurs during the execution of the retrieve operation.
     *                            This includes cases where the ID is missing or empty, or if an IOException occurs.
     */
    @Override
    protected void executeGet(GetRequest getRequest, OperationResponse operationResponse) {
        ObjectIdData input = getRequest.getObjectId();
        try (CloseableHttpClient client = HttpClientFactory.createHttpClient()) {
            List<Map.Entry<String, String>> headers =
                    ((HubspotcrmOperationConnection) this.getConnection()).getHeaders();
            String operationType = getContext().getOperationType().name();
            String idType = getContext().getOperationProperties().getProperty(ID_TYPE);
            String path = String.valueOf(buildURLPath(findAndValidateObjectId(input), idType));
            retrieveObject(operationType, path, client, ((HubspotcrmOperationConnection) this.getConnection()), headers,
                    operationResponse, input);
        } catch (IOException | IllegalArgumentException e) {
            input.getLogger().log(Level.SEVERE, "Failed executing retrieve operation: {0}", e.getMessage());
            ResponseUtil.addExceptionFailure(operationResponse, input, e);
        }
    }

    /**
     * Retrieves a record from Hubspot CRM using the specified parameters.
     *
     * @param operationType                 The type of operation to perform
     * @param path                          The API endpoint path for the request
     * @param client                        The HTTP client used to make the request
     * @param hubspotcrmOperationConnection The Hubspot CRM connection details
     * @param headers                       List of HTTP headers to be included in the request
     * @param operationResponse             Container for the operation's response data
     * @param data                          Object containing identification and logging data
     * @throws IOException If an I/O error occurs during the HTTP request
     *                     or response processing
     */
    private static void retrieveObject(String operationType, String path, CloseableHttpClient client,
            HubspotcrmOperationConnection hubspotcrmOperationConnection, List<Map.Entry<String, String>> headers,
            OperationResponse operationResponse, ObjectIdData data) throws IOException {
        try (CloseableHttpResponse response = ExecutionUtils.execute(operationType, path, client, null,
                hubspotcrmOperationConnection.getUrl(), headers, data.getLogger())) {
            extractResponse(operationResponse, data, response);
        }
    }

    /**
     * Extracts and processes the HTTP response to create an appropriate operation response.
     * This method handles both successful (200 OK) and non-successful HTTP responses.
     *
     * @param operationResponse The operation response object to be populated with the result
     * @param objectIdData      The object ID data containing the identifier of the requested resource
     * @param response          The HTTP response received from the server
     * @throws IOException If an error occurs while reading the response content
     */
    private static void extractResponse(OperationResponse operationResponse, ObjectIdData objectIdData,
            CloseableHttpResponse response) throws IOException {
        if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
            operationResponse.addResult(objectIdData, OperationStatus.SUCCESS,
                    response.getStatusLine().getReasonPhrase(), StringUtil.EMPTY_STRING,
                    ResponseUtil.toPayload(response.getEntity().getContent()));
        } else if (HttpStatus.SC_NOT_FOUND == response.getStatusLine().getStatusCode()) {
            operationResponse.addEmptyResult(objectIdData, OperationStatus.SUCCESS,
                    response.getStatusLine().getReasonPhrase(), null);
        } else {
            operationResponse.addErrorResult(objectIdData, OperationStatus.FAILURE,
                    String.valueOf(response.getStatusLine().getStatusCode()),
                    response.getStatusLine().getReasonPhrase(), null);
        }
    }

    /**
     * Constructs the complete URL path with query parameters for HubSpot API requests.
     *
     * @param objectId The ID of the object to retrieve (can be record ID or email)
     * @param idType   The type of ID being used (email or standard ID)
     * @return StringBuilder The constructed URL path with all necessary parameters
     * @throws IOException If URL encoding fails
     *                     <p>
     *                     The constructed URL includes:
     *                     1. Base path from getPath()
     *                     2. Trimmed object ID
     *                     3. ID type parameter (if email ID)
     *                     4. Properties parameter with encoded cookie value
     *                     5. Associations parameter with encoded default associations
     *                     <p>
     *                     Example outputs:
     *                     For standard ID:
     *                     /crm/v3/objects/contacts/123?properties=firstname,lastname&associations=company
     *                     <p>
     *                     For email ID:
     *                     /crm/v3/objects/contacts/user@example.com&idProperty=email?properties=firstname,
     *                     lastname&associations=company
     *                     <p>
     *                     Note:
     *                     - Uses StringBuilder for efficient string concatenation
     *                     - URL encodes cookie and association values
     *                     - Handles both email and standard ID types differently
     *                     - Appends necessary separators (& and ?) based on ID type
     */
    private StringBuilder buildURLPath(String objectId, String idType) throws IOException {
        return new StringBuilder().append(getPath()).append(objectId.trim()).append(
                (EMAIL_ID.equalsIgnoreCase(idType) ? (ID_TYPE_EMAIL + AMPERSAND_SEPARATOR) : QUERY_SEPARATOR)).append(
                URL_PROPERTIES_KEY).append(ExecutionUtils.urlEncode(getCookie())).append(AMPERSAND_SEPARATOR).append(
                URL_ASSOCIATIONS_KEY).append(ExecutionUtils.urlEncode(ConstantAssociation.getDefaultAssociations()));
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

    /**
     * Retrieves the cookie for the output object definition role.
     *
     * @return The cookie as a {@code String}. Returns {@code null} if not found.
     */
    private String getCookie() {
        return getContext().getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
    }

    /**
     * Validates and returns the object ID, ensuring it meets the required format.
     * If the object ID contains percentage characters, it will be URL encoded.
     *
     * @param input The ObjectIdData containing the object ID to be validated and processed.
     *              Must not be null.
     * @return The processed object ID string. If the ID contains percentage characters,
     * returns the URL encoded version; otherwise, returns the original ID.
     * @throws UnsupportedEncodingException If URL encoding fails
     */
    private static String findAndValidateObjectId(ObjectIdData input) throws UnsupportedEncodingException {
        String objectId = input.getObjectId();
        Args.notBlank(objectId, EMPTY_OBJECT_ID);
        if (objectId.contains(PERCENTAGE)) {
            return ExecutionUtils.urlEncode(objectId);
        }
        return objectId;
    }
}