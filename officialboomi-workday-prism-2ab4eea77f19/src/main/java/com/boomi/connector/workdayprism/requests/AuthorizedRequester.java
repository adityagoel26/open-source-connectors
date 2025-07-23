//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.requests;

import com.boomi.connector.workdayprism.model.AuthProvider;
import com.boomi.connector.workdayprism.utils.Constants;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
abstract class AuthorizedRequester extends Requester{

    private final AuthProvider authProvider;

    /**
     * Creates a new {@link AuthorizedRequester} instance
     *
     * @param httpMethod
     *         the http method to use in the request.
     * @param basePath
     *         the base path for the request.
     * @param maxRetries
     *         how many retries attempts will be allowed.
     * @param authProvider
     *         an {@link AuthProvider} instance.
     */
    AuthorizedRequester(String httpMethod, String basePath, int maxRetries, AuthProvider authProvider) {
        super(httpMethod, basePath, new PrismRetryStrategy(maxRetries));
        this.authProvider = authProvider;
    }

    /**
     * Sets the Authorization Bearer token.
     *
     * @param forceRefresh
     *         forces a renewal of the accessToken stored by the connector cache
     */
    @Override
    void setAuthorization(boolean forceRefresh) {
        String token = authProvider.getAccessToken(forceRefresh);
        setAuthorizationHeaders(Constants.AUTHORIZATION_BEARER_PATTERN, token);
    }
}
