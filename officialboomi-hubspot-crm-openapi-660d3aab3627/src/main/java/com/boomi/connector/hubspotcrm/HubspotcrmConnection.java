// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm;

import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HeaderUtils;
import com.boomi.connector.hubspotcrm.util.HubspotcrmConstant;
import com.boomi.connector.hubspotcrm.util.OpenAPIAction;
import com.boomi.connector.openapi.OpenAPIConnection;
import com.boomi.connector.openapi.util.OpenAPIUtil;
import com.boomi.util.LogUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HubspotcrmConnection<C extends BrowseContext> extends OpenAPIConnection<C> {

    private static final Logger LOG = LogUtil.getLogger(HubspotcrmConnection.class);
    private static final String CUSTOM_HEADERS_PROPERTY = "customHeaders";
    private static final String CUSTOM_SPEC = "custom-specification-hubspot_crm.yaml";

    public HubspotcrmConnection(C context) {
        super(context);
    }

    /**
     * Retrieves the specification for the connection.
     *
     * @return The specification string for the connection.
     */
    @Override
    public String getSpec() {
        return CUSTOM_SPEC;
    }

    /**
     * Retrieves a list of {@link ObjectType} objects based on the OpenAPI paths and operations
     * filtered by the version and operation summary.
     *
     * <p>This method iterates through the available API paths, checks the version specified
     * in the context, and compares it with the version part in the path. If the version matches,
     * it further checks if the operation's summary matches the custom operation type provided in the context.
     * If both conditions are met, an {@link ObjectType} object is created and added to the list, which
     * includes an ID based on the operation and the label set to the operation ID.</p>
     *
     * @return A list of {@link ObjectType} objects containing the filtered OpenAPI actions.
     */
    @Override
    public List<ObjectType> getObjectTypes() {
        List<ObjectType> objects = new ArrayList<>();
        for (Map.Entry<String, PathItem> entrySet : getApi().getPaths().entrySet()) {
            String key = entrySet.getKey();
            // checks the version specified in the context, and compares it with the version part in the path.
            if (getContext().getOperationProperties().getProperty(HubspotcrmConstant.VERSION) != null && (!key.split(
                    HubspotcrmConstant.SLASH)[HubspotcrmConstant.TWO].equalsIgnoreCase(
                    getContext().getOperationProperties().getProperty(HubspotcrmConstant.VERSION)))) {
                continue;
            }
            PathItem path = entrySet.getValue();
            path.readOperationsMap().forEach((httpMethod, operation) -> {
                // Check if the operation's summary(from yaml file) matches the custom operation type (case-insensitive)
                if (operation.getSummary().equalsIgnoreCase(getContext().getCustomOperationType())) {
                    // Create a new OpenAPI action with the HTTP method (GET/POST/etc) and the API path
                    OpenAPIAction action = new OpenAPIAction(httpMethod, entrySet.getKey());
                    objects.add(new ObjectType().withId(action.getId()).withLabel(operation.getOperationId()));
                }
            });
        }
        return objects;
    }

    /**
     * Fetch the {@link Operation} based on the HTTP Method and Path value obtained from the given objectTypeId
     *
     * @param objectTypeId containing the selected HTTP Method and Path
     * @return the Operation
     */
    @Override
    protected Operation getOperation(String objectTypeId) {
        OpenAPIAction action = new OpenAPIAction(objectTypeId);
        return OpenAPIUtil.getOperation(getApi(), action.getMethod(), action.getPath());
    }

    /**
     * Requests a session id from Hubspot and uses it to perform a get request to the provided path.
     *
     * @param client a http client
     * @param path   the get request uri
     * @return the http response
     * @throws IOException if an error occurs executing the request
     */
    public CloseableHttpResponse testConnection(CloseableHttpClient client, String path) throws IOException {
        LOG.log(Level.INFO, "Hubspot Connection URL: {0}", path);
        return ExecutionUtils.execute(HubspotcrmConstant.GET, path, client, null, this.getUrl(),
                getHeaders(), LOG);
    }

    /**
     * Retrieves the headers required for the connection.
     *
     * @return An ArrayList of Map.Entry objects representing the headers.
     */
    public List<Map.Entry<String, String>> getHeaders() {
        Iterable<Map.Entry<String, String>> originalHeaders = Collections.emptyList();
        Map<String, String> customHeaders = getContext().getConnectionProperties().getCustomProperties(
                CUSTOM_HEADERS_PROPERTY);
        return (List<Map.Entry<String, String>>) HeaderUtils.getHeaders(originalHeaders, customHeaders,
                getContext().getConnectionProperties().getProperty(HeaderUtils.API_KEY), getOAuthContext(), null);
    }
}
