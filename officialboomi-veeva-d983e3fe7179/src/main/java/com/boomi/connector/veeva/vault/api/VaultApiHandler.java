// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.vault.api;

import com.boomi.common.apache.http.auth.HttpAuthenticationException;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.util.ConnectorCache;
import com.boomi.connector.util.ConnectorCacheFactory;
import com.boomi.connector.veeva.auth.cache.SessionCache;
import com.boomi.connector.veeva.auth.response.SessionResponse;
import com.boomi.connector.veeva.auth.response.SessionResponseParser;
import com.boomi.connector.veeva.util.HttpClientFactory;
import com.boomi.connector.veeva.util.LogUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.net.URL;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>This class is responsible for structuring the Veeva Vault endpoint and providing Veeva Vault Session IDs; either
 * obtained from the {@link ConnectorCache}, using {@link #getSessionIdFromCache}, or by performing authentication
 * requests to the service, using {@link #requestSessionId}.</p>
 * <p>The requests to get a Session ID can be authenticated with User Credentials or OAuth 2.0 / OpenID Connect. In any
 * case, the connector uses basic authentication in the subsequent API calls, providing the obtained Session ID.</p>
 * <p>When using OAuth 2.0 authentication to get a Session Id, the connector relies on platform to provide a valid (and
 * possibly refreshed) access token. However, if the service response is a failure due to an invalid/expired token, the
 * connector will force a token refresh and retry once.</p>
 */
public class VaultApiHandler {

    private static final Logger LOG = LogUtil.getLogger(VaultApiHandler.class);
    private static final String ERROR_SESSION_ID_FORMAT = "Error obtaining Session ID. %s";
    private static final String VEEVA_ENDPOINT_FORMAT = "https://%s/api/%s/";
    private static final long ONE_MINUTE_IN_MILLIS = 60 * 1000L;

    private final String _vaultDomainName;
    private final String _apiVersion;
    private final String _sessionCacheKey;
    private final long _sessionTimeout;
    private final Function<Boolean, HttpPost> _createSessionRequest;

    VaultApiHandler(String vaultDomainName, String apiVersion, String sessionCacheKey, long sessionTimeout,
            Function<Boolean, HttpPost> createSessionRequest) {
        _vaultDomainName = vaultDomainName;
        _apiVersion = apiVersion;
        _sessionCacheKey = sessionCacheKey;
        _sessionTimeout = sessionTimeout;
        _createSessionRequest = createSessionRequest;
    }

    private static SessionResponse execute(CloseableHttpClient client, HttpPost request) throws IOException {
        LOG.log(Level.FINE, LogUtils.requestMethodAndUriMessage(request));
        CloseableHttpResponse response = client.execute(request);
        try {
            return SessionResponseParser.parse(response);
        } finally {
            IOUtil.closeQuietly(response);
        }
    }

    /**
     * Veeva Vault has an Auth API Burst Limit, which is the number of calls to /api/{version}/auth that can be made in
     * a one (1) minute period. In the unlikely scenario of reaching the limit, wait until the next window.
     */
    private static void backoff() {
        LOG.log(Level.FINE, "Authorization API Burst limit exceeded, retrying in 60 seconds.");
        try {
            Thread.sleep(ONE_MINUTE_IN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Errors occurred while performing retry backoff logic.", e);
        }
    }

    /**
     * Structure the Veeva Vault API endpoint to interact with.
     *
     * @return the Veeva Vault endpoint
     */
    public URL getVaultApiEndpoint() {
        return VaultApiUtils.getUrlOf(String.format(VEEVA_ENDPOINT_FORMAT, _vaultDomainName, _apiVersion));
    }

    /**
     * Returns a session id from an instance of {@link SessionCache}, either a previously created instance which was
     * found in the {@link ConnectorCache} and is still valid or a new instance created by the given cache factory.
     *
     * @param context required to get the connector cache and possibly create a new cache instance
     * @return a session id representing a Veeva session
     */
    public String getSessionIdFromCache(ConnectorContext context) {
        SessionCache sessionCache = ConnectorCache.getCache(_sessionCacheKey, context, getCacheFactory());

        return sessionCache.getSessionId();
    }

    /**
     * Executes a request to Vault Rest API to generate a new session, parses the response and returns the session id.
     *
     * @param client the http client to execute the auth request
     * @return a session id representing a Veeva session
     */
    public String requestSessionId(CloseableHttpClient client) {
        try {
            SessionResponse sessionResponse = execute(client, createRequest(false));

            if (sessionResponse.isFailure()) {
                sessionResponse = retry(client, sessionResponse);
            }

            String sessionId = sessionResponse.getSessionId();
            if (StringUtil.isBlank(sessionId)) {
                throw new HttpAuthenticationException(sessionResponse.extractErrorMessage());
            }

            return sessionId;
        } catch (IOException e) {
            String errorMessage = String.format(ERROR_SESSION_ID_FORMAT, e.getMessage());
            LOG.log(Level.WARNING, errorMessage, e);
            throw new ConnectorException(errorMessage);
        } finally {
            IOUtil.closeQuietly(client);
        }
    }

    /**
     * Returns a string representing the connection configuration, used to store {@link SessionCache} instances.
     *
     * @return key used to cache Veeva Session IDs for a particular connection in the connector cache
     */
    public String getSessionCacheKey() {
        return _sessionCacheKey;
    }

    /**
     * Returns a new {@link HttpPost} request to get a session id from Vault Rest API. The request URI, headers and
     * entity varies depending on the authentication type.
     *
     * @param forceRefreshToken indicates whether this request requires a token refresh (for OAuth 2.0 authentication)
     * @return a new authentication request to get a session id from Veeva Vault
     */
    HttpPost createRequest(boolean forceRefreshToken) {
        return _createSessionRequest.apply(forceRefreshToken);
    }

    private ConnectorCacheFactory<String, SessionCache, ConnectorContext> getCacheFactory() {
        // ConnectorCacheFactory#createCache implementation
        return (sessionCacheKey, context) -> {
            CloseableHttpClient client = HttpClientFactory.createHttpClient(context);
            String sessionId = requestSessionId(client);
            return new SessionCache(sessionCacheKey, sessionId, _sessionTimeout);
        };
    }

    /**
     * Retries an authentication request only once and returns a new {@link SessionResponse}, in two specific scenarios:
     * <li>Using an expired or invalid access token. This only applies to OAuth 2.0 authentication. In this situation,
     * the access token is refresh and the request is retried once.</li>
     * <li>The Auth API Burst Limit was reached. This only applies to User Credentials authentication. In this case, the
     * connector waits one minute, until the next burst-limit period and retry once. This is an unlikely scenario
     * considering that the connector caches sessions and the service delays responses when 50% of the burst limit is
     * reached for the remainder of the burst-limit period.</li>
     * If the conditions to perform a retry are not met, returns the provided {@link SessionResponse}.
     *
     * @param client          the client for executing an additional auth request, if applicable
     * @param sessionResponse the parsed response to determine if the request should be retried
     * @return a new {@link SessionResponse} if a retried is performed, otherwise the provided {@link SessionResponse}
     * @throws IOException if an error occurs executing the request or parsing the response
     */
    private SessionResponse retry(CloseableHttpClient client, SessionResponse sessionResponse) throws IOException {
        HttpPost request = null;

        if (sessionResponse.isOAuthError()) {
            request = createRequest(true);
        }

        if (sessionResponse.isApiLimitExceededError()) {
            backoff();
            request = createRequest(false);
        }

        return request != null ? execute(client, request) : sessionResponse;
    }
}
