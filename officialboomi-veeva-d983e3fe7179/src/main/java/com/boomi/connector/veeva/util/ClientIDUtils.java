// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.util;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.util.StringUtil;

import org.apache.http.client.methods.HttpRequestBase;

/**
 * Utility class for building the Veeva Client ID
 */
public class ClientIDUtils {

    /**
     * Veeva Client ID header name
     */
    public static final String VEEVA_CLIENT_ID_HEADER = "X-VaultAPI-ClientID";

    private static final String CLIENT_ID_KEY = "veevaClientID";
    private static final String DEFAULT_CLIENT_ID = "Boomi";
    private static final String CLIENT_ID_PREFIX = "Boomi_";

    private ClientIDUtils() {
    }

    /**
     * Build the Veeva Client ID from the given connection properties.
     * <p>
     * If the Client ID property is not defined, this method returns the value 'Boomi'. Otherwise, a concatenation of
     * 'Boomi_' and the defined value is returned.
     * <p>
     * The input value is trimmed before using it.
     *
     * @param connectionProperties to extract the Veeva Client ID
     * @return the Veeva Client ID
     */
    public static String buildClientID(PropertyMap connectionProperties) {
        String clientID = connectionProperties.getProperty(CLIENT_ID_KEY);
        clientID = StringUtil.trimToEmpty(clientID);

        if (StringUtil.isEmpty(clientID)) {
            return DEFAULT_CLIENT_ID;
        }

        return CLIENT_ID_PREFIX + clientID;
    }

    /**
     * Build the Veeva Client ID from the given context and set it as a header in the provided http request.
     *
     * @param httpRequest the request to Veeva Vault API
     * @param context     the connector context with the connection properties to build the client id
     */
    public static void setClientID(HttpRequestBase httpRequest, ConnectorContext context) {
        String clientID = ClientIDUtils.buildClientID(context.getConnectionProperties());
        httpRequest.setHeader(ClientIDUtils.VEEVA_CLIENT_ID_HEADER, clientID);
    }
}
