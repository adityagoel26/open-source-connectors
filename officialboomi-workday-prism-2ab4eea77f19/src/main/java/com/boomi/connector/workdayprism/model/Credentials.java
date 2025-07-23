//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.model;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.Base64Util;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;

import org.apache.commons.codec.Charsets;

import java.net.URL;
import java.nio.file.Paths;

/**
 * Class containing Workday Connection Params
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class Credentials {
    private static final String AUTHORIZATION_DELIMITER = ":";
    private static final String BASE_ENDPOINT_PATTERN = "%s://%s";
    private static final int TENANT_POSITION = 3;

    private static final String INVALID_ENDPOINT_ERROR = "invalid endpoint";

    private final String refreshToken;
    private final String baseEndpoint;
    private final String tenant;
    private final String basicAuth;

    /**
     * Creates a new {@link Credentials} instance
     *
     * @param endpoint
     *         the complete endpoint value as read in the api credentials in Workday
     * @param clientId
     *         the clientId of the credentials used in the connection
     * @param clientSecret
     *         the secret of the credentials used in the connection
     * @param refreshToken
     *         the refreshToken in Workday Prism is static and needs to be copied from their site.
     */
    public Credentials(String endpoint, String clientId, String clientSecret, String refreshToken) {
        try {
            URL uri = new URL(endpoint);
            this.baseEndpoint = String.format(BASE_ENDPOINT_PATTERN, uri.getProtocol(), uri.getHost());
            this.tenant = Paths.get(uri.getPath()).getName(TENANT_POSITION).toString();
        }
        catch (Exception e) {
            throw new ConnectorException(INVALID_ENDPOINT_ERROR, e);
        }

        this.refreshToken = refreshToken;
        this.basicAuth = buildCredentials(clientId, clientSecret);
    }

    /** Returns a Base64Util encoded String by merging client Id and client secret, delimited by ':'
     * @param clientId
     * @param clientSecret
     * @return String 
     */
    private static String buildCredentials(String clientId, String clientSecret) {
        String plainCredentials = StringUtil.join(AUTHORIZATION_DELIMITER, clientId, clientSecret);
        return Base64Util.encodeToUnformattedString(plainCredentials, Charsets.UTF_8);
    }

    /**
     * Getter method to access the basic auth string for this credentials. It will a Base64 encoded String with the
     * following format: {clientId}:{clientSecret}
     *
     * @return a String value of the basic authorization.
     */
    public String getBasicAuth() {
        return basicAuth;
    }

    /**
     * Getter method to access the base endpoint.
     *
     * @return a String value of the endpoint.
     */
    String getBaseEndpoint() {
        return baseEndpoint;
    }

    /**
     * Getter method to access the tenant id path of the account.
     *
     * @return a String value for the tenant id.
     */
    String getTenant() {
        return tenant;
    }

    /**
     * Getter method to access the refresh token
     *
     * @return a String value of the refresh token.
     */
    public String getRefreshToken() {
        return refreshToken;
    }

    /**
     * Getter method to access the base url by adding the host and tenant id to the provided pattern
     *
     * @return a String value of the base url
     */
    public String getBasePath(String path){
        return URLUtil.makeUrlString(baseEndpoint, path, tenant);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Credentials)) {
            return false;
        }

        Credentials that = (Credentials) o;
        return new EqualsBuilder()
                .append(refreshToken, that.refreshToken)
                .append(baseEndpoint, that.baseEndpoint)
                .append(tenant, that.tenant)
                .append(basicAuth, that.basicAuth)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 3)
                .append(refreshToken)
                .append(baseEndpoint)
                .append(tenant)
                .append(basicAuth)
                .toHashCode();
    }
}
