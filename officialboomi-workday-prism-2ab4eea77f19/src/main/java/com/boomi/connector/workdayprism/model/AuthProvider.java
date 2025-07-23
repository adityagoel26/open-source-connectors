//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.model;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.util.ConnectorCache;

/**
 * This class is responsible of getting a new Access Token from the Workday Prism API and store it into connector cache
 * before returning its String representation upon a call to {@link AuthProvider#getAccessToken(boolean)}.
 *
 * The TTL of the provided token is not documented by Workday and therefore this class will return the same token until
 * its renewal is forcing by passing true value to the forceRefresh parameter of the aforementioned method.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class AuthProvider {
    private static final String API_ENDPOINT = "endpoint";
    private static final String CLIENT_ID = "clientId";
    private static final String CLIENT_SECRET = "clientSecret";
    private static final String REFRESH_TOKEN = "refreshToken";

    private final Credentials credentials;
    private final BrowseContext context;

    /**
     * Creates a new {@link AuthProvider} instance
     *
     * @param context
     *         a {@link BrowseContext} instance
     */
    public AuthProvider(BrowseContext context) {
        credentials = buildCredentials(context);
        this.context = context;
    }

    /**
     * Returns a full url context including protocol, host and base path.
     *
     * @param urlPattern
     *         a String value with the API path to which the host and tenant id must be appended
     */
    public String getBasePath(String urlPattern) {
        return credentials.getBasePath(urlPattern);
    }

    /**
     * Returns an Access Token for the credentials configured in the {@link BrowseContext} passed during the
     * construction of this class. The token will be stored in cache until this method is called with a true value to
     * force it renewal.
     *
     * @param forceRefresh
     *         a boolean value to indicate if the token must be refreshed.
     */
    public String getAccessToken(boolean forceRefresh) {
        if (forceRefresh) {
        	ConnectorCache.clearCache(credentials, context);
        }

        Token cachedToken = ConnectorCache.getCache(credentials, context, Token.tokenFactory());
        return cachedToken.getAccessToken();
    }

    /** Initialize the Credentials object by fetching values from PropertyMap obtained from Application Context
     * @param context an instance of BrowseContext
     * @return an instance of Credentials
     */
    private static Credentials buildCredentials(BrowseContext context) {
        PropertyMap connProps = context.getConnectionProperties();

        String endpoint = connProps.getProperty(API_ENDPOINT);
        String clientId = connProps.getProperty(CLIENT_ID);
        String secret = connProps.getProperty(CLIENT_SECRET);
        String refreshToken = connProps.getProperty(REFRESH_TOKEN);

        return new Credentials(endpoint, clientId, secret, refreshToken);
    }


}
