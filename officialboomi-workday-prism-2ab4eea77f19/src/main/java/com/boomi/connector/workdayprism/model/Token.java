//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.model;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.util.ConnectorCache;
import com.boomi.connector.util.ConnectorCacheFactory;
import com.boomi.connector.workdayprism.requests.TokenRequester;
import com.boomi.connector.workdayprism.responses.TokenResponse;

import java.io.IOException;

/**
 * Token class is an implementation of {@link ConnectorCache} as part of mechanism to reduce the amount of
 * requests needed to ensure a valid token is available.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class Token extends ConnectorCache<Credentials> {

    private static final String ERROR_GET_TOKEN = "couldn't get access token";
    private final String accessToken;

    /**
     * Creates a new {@link Token} instance.
     * It will be invoked by {@link AuthProvider#getAccessToken(boolean)} and it should not be directly used.
     *
     * @param key
     *         a {@link Credentials} instance
     * @param accessToken
     *         the access token as received from the API.
     */
    private Token(Credentials key, String accessToken) {
        super(key);
        this.accessToken = accessToken;
    }

    /**
     * Getter method to retrieve the access token.
     *
     * @return a String value for the access token.
     */
    String getAccessToken() {
        return accessToken;
    }

    /**
     * Factory method to create a new {@link ConnectorCacheFactory}<{@link Credentials}, {@link Token}>
     *
     * @return a new {@link ConnectorCacheFactory}
     */
    static ConnectorCacheFactory<Credentials, Token, BrowseContext> tokenFactory() {
        return new ConnectorCacheFactory<Credentials, Token, BrowseContext>() {
            @Override
            public Token createCache(Credentials key, BrowseContext context) {
                try {
                    TokenResponse accessTokenInner = new TokenRequester(key).get();
                    return new Token(key, accessTokenInner.getAccessToken());
                }
                catch (IOException e) {
                    throw new ConnectorException(ERROR_GET_TOKEN, e);
                }
            }
        };
    }
}
