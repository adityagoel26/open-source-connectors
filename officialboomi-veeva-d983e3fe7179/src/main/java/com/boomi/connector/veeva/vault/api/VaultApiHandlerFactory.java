// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.vault.api;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.veeva.util.ClientIDUtils;
import com.boomi.util.StringUtil;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

/**
 * This class creates {@link VaultApiHandler} instances which holds data required to build the Veeva Vault API
 * endpoints and to obtain Veeva Vault Session IDs, either from cache or by requests to the Veeva Vault API. The data
 * required to get a session id varies depending on the {@link AuthenticationType} selected. Note that the latter is
 * only used to obtain a Veeva Vault Session ID and the connector uses basic authentication in the subsequent requests.
 */
public class VaultApiHandlerFactory {

    private static final String VEEVA_ENDPOINT_AUTH_FORMAT_USER_CREDENTIALS = "https://%s/api/%s/auth";
    private static final String VEEVA_ENDPOINT_AUTH_FORMAT_OAUTH = "https://login.veevavault.com/auth/oauth/session/%s";
    private static final String AUTHENTICATION_TYPE = "authenticationType";
    private static final String AUTHORIZATION = "Authorization";
    private static final String SEPARATOR = ",";

    /**
     * Creates a new {@link VaultApiHandler} instance containing the necessary data to obtain Veeva Vault endpoints
     * and session ids. The data required to achieve this varies depending on the authentication method selected.
     *
     * @param context the context containing the connection and operation properties
     * @return a vault api handler for the corresponding authentication type
     */
    public static VaultApiHandler create(BrowseContext context) {
        PropertyMap connectionProperties = context.getConnectionProperties();
        String username = VaultApiUtils.username(connectionProperties);
        String clientId = ClientIDUtils.buildClientID(connectionProperties);
        String apiVersion = VaultApiUtils.apiVersion(connectionProperties, context.getOperationProperties());
        long sessionTimeout = VaultApiUtils.timeout(connectionProperties);

        switch (getAuthenticationType(connectionProperties)) {
            case OAUTH_2:
                return oAuth2SessionProvider(connectionProperties, username, clientId, sessionTimeout, apiVersion);

            case USER_CREDENTIALS:
            default:
                return userCredentialsSessionProvider(connectionProperties, username, clientId, sessionTimeout,
                        apiVersion);
        }
    }

    private static VaultApiHandler oAuth2SessionProvider(PropertyMap connectionProperties, String username,
            String clientId, long sessionTimeout, String apiVersion) {
        // build the session cache key
        String vaultDNS = VaultApiUtils.vaultDNS(connectionProperties);
        String profileID = VaultApiUtils.profileId(connectionProperties);
        String authServerClientApplicationID = VaultApiUtils.authServerClientApplicationId(connectionProperties);

        String sessionCacheKey = StringUtil.join(SEPARATOR, vaultDNS, username, profileID,
                authServerClientApplicationID, sessionTimeout);

        // build the session request factory
        String sessionRequestUri = getAuthEndpointOAuth(profileID);
        OAuth2Context oAuth2Context = VaultApiUtils.oAuth2Context(connectionProperties);
        HttpEntity entity = getOAuth2Entity(vaultDNS, authServerClientApplicationID);

        Function<Boolean, HttpPost> sessionRequestFactory = (Boolean forceRefreshToken) -> {
            HttpPost request = new HttpPost(sessionRequestUri);
            request.addHeader(AUTHORIZATION, "Bearer " + getOAuth2AccessToken(oAuth2Context, forceRefreshToken));
            request.addHeader(ClientIDUtils.VEEVA_CLIENT_ID_HEADER, clientId);
            request.setEntity(entity);

            return request;
        };

        return new VaultApiHandler(vaultDNS, apiVersion, sessionCacheKey, sessionTimeout, sessionRequestFactory);
    }

    private static VaultApiHandler userCredentialsSessionProvider(PropertyMap connectionProperties, String username,
            String clientID, long sessionTimeout, String apiVersion) {
        // build the session cache key
        String vaultSubdomain = VaultApiUtils.vaultSubdomain(connectionProperties);
        String sessionCacheKey = StringUtil.join(SEPARATOR, vaultSubdomain, username, sessionTimeout);

        // build the session request factory
        String sessionRequestUri = getAuthEndpointUserCredentials(vaultSubdomain, apiVersion);
        HttpEntity entity = getUserCredentialsEntity(connectionProperties, username);

        Function<Boolean, HttpPost> sessionRequestFactory = (Boolean forceRefreshToken) -> {
            HttpPost request = new HttpPost(sessionRequestUri);
            request.addHeader(ClientIDUtils.VEEVA_CLIENT_ID_HEADER, clientID);
            request.setEntity(entity);

            return request;
        };

        return new VaultApiHandler(vaultSubdomain, apiVersion, sessionCacheKey, sessionTimeout, sessionRequestFactory);
    }

    /**
     * This method returns an up-to-date access token string, even when forceUpdate=false, provided the authentication
     * service is configured to provide refresh tokens.
     *
     * @param oAuth2Context to retrieve updated access tokens
     * @param forceUpdate   boolean indicating whether to force an access token refresh
     * @return an up-to-date access token string
     */
    private static String getOAuth2AccessToken(OAuth2Context oAuth2Context, boolean forceUpdate) {
        try {
            return oAuth2Context.getOAuth2Token(forceUpdate).getAccessToken();
        } catch (IOException e) {
            throw new ConnectorException("Error getting OAuth2 token.", e);
        }
    }

    private static HttpEntity getOAuth2Entity(String vaultDNS, String authServerClientApplicationID) {
        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
        if (StringUtil.isNotBlank(vaultDNS)) {
            entityBuilder.addTextBody("vaultDNS", vaultDNS);
        }
        if (StringUtil.isNotBlank(authServerClientApplicationID)) {
            entityBuilder.addTextBody("client_id", authServerClientApplicationID);
        }
        return entityBuilder.build();
    }

    private static UrlEncodedFormEntity getUserCredentialsEntity(PropertyMap connectionProperties, String username) {
        Collection<NameValuePair> parameters = new ArrayList<>();
        parameters.add(new BasicNameValuePair("username", username));
        parameters.add(new BasicNameValuePair("password", VaultApiUtils.password(connectionProperties)));

        return new UrlEncodedFormEntity(parameters, StringUtil.UTF8_CHARSET);
    }

    private static AuthenticationType getAuthenticationType(PropertyMap connectionProperties) {
        return AuthenticationType.valueOf(
                connectionProperties.getProperty(AUTHENTICATION_TYPE, AuthenticationType.USER_CREDENTIALS.name()));
    }

    private static String getAuthEndpointUserCredentials(String vaultDomainName, String apiVersion) {
        return String.format(VEEVA_ENDPOINT_AUTH_FORMAT_USER_CREDENTIALS, vaultDomainName, apiVersion);
    }

    private static String getAuthEndpointOAuth(String profileId) {
        return String.format(VEEVA_ENDPOINT_AUTH_FORMAT_OAUTH, profileId);
    }

    private enum AuthenticationType {USER_CREDENTIALS, OAUTH_2}
}
