// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.properties;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.PropertyMap;
import com.boomi.salesforce.rest.authenticator.AuthenticationType;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.util.StringUtil;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple helper class that contains the properties needed for the connection and the connector cache
 */
public class ConnectionProperties {

    public static final long DEFAULT_CONCURRENT_CONNECTIONS = 5L;
    private static final int MAX_CONCURRENT_CONNECTIONS_ALLOWED = 25;
    private static final String DEFAULT_SF_LOGIN_URL = "https://login.salesforce.com/services/Soap/u/48.0";
    private final PropertyMap _connectionProperties;
    private final ConcurrentMap<Object, Object> _connectorCache;
    private final Logger _logger;
    // globally uniquely identifier for caching prefixes
    private final String _uuid;

    public ConnectionProperties(PropertyMap connectionProperties, ConcurrentMap<Object, Object> connectorCache,
            Logger logger) {
        _connectionProperties = connectionProperties;
        _connectorCache = connectorCache;
        _logger = logger;
        if (MAX_CONCURRENT_CONNECTIONS_ALLOWED < getMaxConcurrentConnections() || getMaxConcurrentConnections() < 1) {
            throw new ConnectorException("Maximum concurrent connections field must set to be between 1 and 25");
        }
        _uuid = UUID.randomUUID().toString();
    }

    public String getUsername() {
        return _connectionProperties.getProperty("username");
    }

    public String getPassword() {
        return _connectionProperties.getProperty("password", "");
    }

    public AuthenticationType getAuthenticationType() {
        return AuthenticationType.valueOf(_connectionProperties.getProperty("authenticationType"));
    }

    public final int getMaxConcurrentConnections() {
        long concurrentConnections = _connectionProperties.getLongProperty("concurrent",
                DEFAULT_CONCURRENT_CONNECTIONS);
        return (int) Long.min(concurrentConnections, Integer.MAX_VALUE);
    }

    public URI getURL() {
        try {
            String url = _connectionProperties.getProperty("url");
            if (StringUtil.isEmpty(url)) {
                throw new ConnectorException("URL to Salesforce REST APIs cannot be empty");
            }
            URI uri = new URI(StringUtils.removeEnd(url, "/"));
            if (!uri.isAbsolute()) {
                throw new ConnectorException("URL to Salesforce REST APIs is malformed.");
            }
            return uri;
        } catch (URISyntaxException e) {
            throw new ConnectorException("[Errors occurred while parsing connection URL] " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves the authentication URL from the connection properties.
     *
     * @return URI object containing the authentication URL if present in the properties,
     * null if the authentication URL is not specified
     * @throws ConnectorException if there is an error parsing the URL into a URI
     */
    public URI getAuthenticationUrl() {
        String authUrl = StringUtil.defaultIfBlank(_connectionProperties.getProperty("authenticationUrl"),
                DEFAULT_SF_LOGIN_URL).trim();
        try {
            return new URI(authUrl);
        } catch (URISyntaxException e) {
            throw new ConnectorException("[Errors occurred while parsing authentication URL] " + e.getMessage(), e);
        }
    }

    public void logFine(String string) {
        if (_logger != null) {
            _logger.fine(string);
        }
    }

    public void logInfo(String message) {
        if (_logger != null && StringUtil.isNotBlank(message)) {
            _logger.info(message);
        }
    }

    /**
     * Generates a unique cache key based on the authentication configuration.
     *
     * @param key            The base key to be made unique
     * @param isGlobalUnique If true, the key will be made unique using only the UUID.
     *                       If false, the key will be made unique using authentication-specific parameters
     * @return A unique cache key string combining the base key with authentication-specific identifiers
     * @throws UnsupportedOperationException if the authentication type is not supported
     */
    private String getUniqueCacheKey(String key, boolean isGlobalUnique) {
        if (isGlobalUnique) {
            return _uuid + "." + key;
        }

        String userIdentifier;
        URI url;
        switch (getAuthenticationType()) {
            case USER_CREDENTIALS:
                userIdentifier = getUsername();
                url = getAuthenticationUrl();
                break;
            case OAUTH_2:
                userIdentifier = getClientID();
                url = getURL();
                break;
            default:
                throw new UnsupportedOperationException("unknown authentication type: " + getAuthenticationType());
        }

        return String.format("%s.%s.%s", key, userIdentifier, url);
    }

    private String getClientID() {
        return getOAuth2Context().getClientId();
    }

    /**
     * Returns String will be used as a key in the connector Cache to define SObject Metadata for a given operationx
     */
    private String getSObjectCacheKey(String sobjectName, String operation, boolean isGlobalUnique) {
        return getUniqueCacheKey(sobjectName + "." + operation, isGlobalUnique);
    }

    /**
     * Returns true if the target SObjectModel object is stored in cache for specific operation
     */
    public boolean isSObjectCached(String sobjectName, String operation, boolean isGlobalUnique) {
        return _connectorCache.containsKey(getSObjectCacheKey(sobjectName, operation, isGlobalUnique));
    }

    public SObjectModel getSObject(String sobjectName, String operation, boolean isGlobalUnique) {
        return (SObjectModel) _connectorCache.get(getSObjectCacheKey(sobjectName, operation, isGlobalUnique));
    }

    public void cacheSObject(String sobjectName, String operation, SObjectModel sobject) {
        _connectorCache.put(getSObjectCacheKey(sobjectName, operation, true), sobject);
        _connectorCache.put(getSObjectCacheKey(sobjectName, operation, false), sobject);
    }

    /**
     * Returns the OAuth2Context will be used in OAuth2 authentication
     */
    public OAuth2Context getOAuth2Context() {
        return _connectionProperties.getOAuth2Context("salesforceOauth");
    }

    public void logWarning(Supplier<String> message, Throwable t) {
        if (_logger != null) {
            _logger.log(Level.WARNING, t, message);
        }
    }

    public void logWarning(String message) {
        if (_logger != null) {
            _logger.log(Level.WARNING, message);
        }
    }

    public void cacheSession(String sessionID) {
        _connectorCache.put(getUniqueCacheKey("credentials", false), sessionID);
    }

    public String getSessionFromCache() {
        return (String) _connectorCache.get(getUniqueCacheKey("credentials", false));
    }
}
