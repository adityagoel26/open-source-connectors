// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm;

import com.boomi.connector.api.OperationContext;
import com.boomi.connector.hubspotcrm.util.HeaderUtils;
import com.boomi.connector.openapi.OpenAPIOperationConnection;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HubspotcrmOperationConnection extends OpenAPIOperationConnection {

    private static final String CUSTOM_HEADERS_PROPERTY = "customHeaders";

    /**
     * Constructs a new HubspotOperationConnection instance with the given OperationContext.
     *
     * @param context The OperationContext for this connection.
     */
    public HubspotcrmOperationConnection(OperationContext context) {
        super(context);
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