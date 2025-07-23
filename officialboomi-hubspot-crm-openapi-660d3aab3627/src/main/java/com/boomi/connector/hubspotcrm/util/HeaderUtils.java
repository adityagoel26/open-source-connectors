// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.ObjectData;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;

/**
 * Utility class for handling HTTP headers.
 */
public class HeaderUtils {

    private static final String AUTHORIZATION = "Authorization";
    private static final String BEARER = "Bearer ";
    private static final String CONTENT_TYPE = "content-type";
    private static final String APPLICATION_JSON = "application/json ";
    public static final String API_KEY = "apiKey";

    private HeaderUtils() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    /**
     * Generates a list of HTTP headers by combining the original headers, custom headers,
     * and authentication headers (either API key or OAuth token).
     *
     * @param originalHeaders The original set of headers.
     * @param customHeaders   Additional custom headers to be added.
     * @param apiKey          The API key for authentication, if applicable.
     * @param oAuthContext    The OAuth context for obtaining the access token, if applicable.
     * @return An iterable containing the combined set of headers.
     */
    public static Iterable<Map.Entry<String, String>> getHeaders(Iterable<Map.Entry<String, String>> originalHeaders,
            Map<String, String> customHeaders, String apiKey, OAuth2Context oAuthContext, ObjectData request) {
        ArrayList<Map.Entry<String, String>> headerList = new ArrayList<>();
        headerList.add(new AbstractMap.SimpleEntry<>(CONTENT_TYPE, APPLICATION_JSON));
        if (StringUtil.isNotBlank(apiKey)) {
            headerList.add(new AbstractMap.SimpleEntry<>(AUTHORIZATION, BEARER + apiKey));
        } else {
            getOAuth2Token(headerList, oAuthContext, request);
        }
        // Add other custom headers
        originalHeaders.forEach(headerList::add);
        if (!CollectionUtil.isEmpty(customHeaders)) {
            customHeaders.forEach((key, value) -> headerList.add(new AbstractMap.SimpleEntry<>(key, value)));
        }
        return headerList;
    }

    /**
     * Retrieves an OAuth2 bearer token from the {@link OAuth2Context} and adds it to the header list.
     * Logs an error and throws a {@link ConnectorException} if an IOException occurs.
     *
     * @param headerList   List of headers to update with the bearer token.
     * @param oAuthContext Context for obtaining the OAuth2 token.
     * @param request      ObjectData for logging errors.
     */
    private static void getOAuth2Token(ArrayList<Map.Entry<String, String>> headerList, OAuth2Context oAuthContext,
            ObjectData request) {
        try {
            String bearerToken = oAuthContext.getOAuth2Token(false).getAccessToken();
            // Add the bearer token header if it's not blank
            if (StringUtil.isNotBlank(bearerToken)) {
                headerList.add(new AbstractMap.SimpleEntry<>(AUTHORIZATION, BEARER + bearerToken));
            }
        } catch (IOException e) {
            if (request != null) {
                request.getLogger().log(Level.SEVERE, "IOException occurred while getting getOAuth2Token", e);
            }
            throw new ConnectorException("IOException occurred while getting getOAuth2Token", e);
        }
    }
}
