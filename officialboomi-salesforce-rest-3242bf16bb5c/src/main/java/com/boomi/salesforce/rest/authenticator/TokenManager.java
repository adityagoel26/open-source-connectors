// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.authenticator;

import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.request.AuthenticationRequestExecutor;
import com.boomi.salesforce.rest.request.RequestBuilder;

/**
 * Interface responsible for managing session either by using SOAP API or access token using OAuth2
 */
public interface TokenManager {

    /**
     * return the current access token
     *
     * @return current access token
     */
    String getAccessToken();

    /**
     * generate and return access token
     *
     * @return the new generated access token
     */
    String generateAccessToken();

    /**
     * Factory method to build the appropriate TokenManager depending on the authentication type (user credentials or
     * OAuth 2.0)
     *
     * @param connectionProperties         containing the connection settings
     * @param requestBuilder               the request builder
     * @param authenticatorRequestExecutor the authenticator executor
     * @return an instance of TokenManager
     */
    static TokenManager getTokenManager(ConnectionProperties connectionProperties, RequestBuilder requestBuilder,
            AuthenticationRequestExecutor authenticatorRequestExecutor) {
        AuthenticationType authenticationType = connectionProperties.getAuthenticationType();
        switch (authenticationType) {
            case USER_CREDENTIALS:
                connectionProperties.logFine("Initializing Basic Authentication");
                return new SFSoapAuthenticator(requestBuilder, authenticatorRequestExecutor, connectionProperties);
            case OAUTH_2:
                connectionProperties.logFine("Initializing OAuth2 Authentication");
                return new SFOAuth2Authenticator(connectionProperties.getOAuth2Context());
            default:
                throw new UnsupportedOperationException(authenticationType + " is not supported");
        }
    }
}
