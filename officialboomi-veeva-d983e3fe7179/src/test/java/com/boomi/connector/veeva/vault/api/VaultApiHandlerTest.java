// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.vault.api;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.util.TestUtil;

import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VaultApiHandlerTest {

    private static final String _sessionResponseSuccess =
            "{\n" + "\t\"responseStatus\": \"SUCCESS\",\n" + "\t\"sessionId\": " + "\"sessionIdValue\",\n"
            + "\t\"userId\": 9289999,\n" + "\t\"vaultIds\": [{\n" + "\t\t\"id\": 73633,\n"
            + "\t\t\"name\": \"clinical01\",\n" + "\t\t\"url\": \"https://subdomain.veevavault.com/api\"\n" + "\t}],\n"
            + "\t\"vaultId\": 73633\n" + "}";

    private static final PropertyMap _connectionPropertiesOAuth2 = getConnectionPropertiesOAuth();
    private static final PropertyMap _connectionPropertiesUserCredentials = getConnectionPropertiesUserCredentials();
    private BrowseContext _browseContext;

    @BeforeAll
    static void setup() {
        TestUtil.disableBoomiLog();
    }

    private static String getThrowableMessage(Executable executable) {
        Throwable t = Assertions.assertThrows(ConnectorException.class, executable);
        return t.getMessage();
    }

    private static PropertyMap getConnectionPropertiesOAuth() {
        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("authenticationType", "OAUTH_2");
        connectionProperties.put("apiVersion", "23.3");
        connectionProperties.put("sessionTimeout", 10L);
        connectionProperties.put("username", "username");
        connectionProperties.put("oathProfileId", "_profile-id");
        connectionProperties.put("vaultDNS", "subdomain.veevavault.com");
        connectionProperties.put("authServerClientApplicationId", "clientAppId");
        connectionProperties.put("veevaOauth", mock(OAuth2Context.class, RETURNS_DEEP_STUBS));
        return connectionProperties;
    }

    private static PropertyMap getConnectionPropertiesUserCredentials() {
        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("authenticationType", "USER_CREDENTIALS");
        connectionProperties.put("apiVersion", "23.3");
        connectionProperties.put("vaultSubdomain", "subdomain.veevavault.com");
        connectionProperties.put("username", "username");
        connectionProperties.put("password", "the password");
        connectionProperties.put("sessionTimeout", 10L);
        return connectionProperties;
    }

    @BeforeEach
    void prepare() {
        _browseContext = mock(BrowseContext.class);
        when(_browseContext.getOperationProperties()).thenReturn(new MutablePropertyMap());
    }

    @Test
    void buildCacheKeyUserCredentialsTest() {
        final String cacheKeyExpected = "subdomain.veevavault.com,username,10";
        when(_browseContext.getConnectionProperties()).thenReturn(_connectionPropertiesUserCredentials);

        VaultApiHandler vaultApiHandler = VaultApiHandlerFactory.create(_browseContext);

        assertNotNull(vaultApiHandler.getSessionCacheKey());
        assertEquals(cacheKeyExpected, vaultApiHandler.getSessionCacheKey());
    }

    @Test
    void buildCacheKeyOAuthTest() throws IOException {
        final String cacheKeyExpected = "subdomain.veevavault.com,username,_profile-id,clientAppId,10";

        OAuth2Context oAuth2Context = mock(OAuth2Context.class, RETURNS_DEEP_STUBS);
        _connectionPropertiesOAuth2.put("veevaOauth", oAuth2Context);

        when(oAuth2Context.getOAuth2Token(false).getAccessToken()).thenReturn("accessToken");
        when(_browseContext.getConnectionProperties()).thenReturn(_connectionPropertiesOAuth2);

        VaultApiHandler vaultApiHandler = VaultApiHandlerFactory.create(_browseContext);

        assertNotNull(vaultApiHandler.getSessionCacheKey());
        assertEquals(cacheKeyExpected, vaultApiHandler.getSessionCacheKey());
    }

    @Test
    void requestSessionIdUserCredentialsTest() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(_sessionResponseSuccess.getBytes(StandardCharsets.UTF_8));

        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        StatusLine statusLine = mock(StatusLine.class);

        when(_browseContext.getConnectionProperties()).thenReturn(_connectionPropertiesUserCredentials);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(response.getEntity().getContentLength()).thenReturn(1L);
        when(response.getEntity().getContent()).thenReturn(inputStream);
        when(client.execute(any(HttpPost.class))).thenReturn(response);

        VaultApiHandler vaultApiHandler = VaultApiHandlerFactory.create(_browseContext);
        assertEquals("sessionIdValue", vaultApiHandler.requestSessionId(client));
    }

    @Test
    void requestSessionIdUserCredentialsThrowsExceptionTest() throws IOException {
        String authResponseString = "{\n" + "\t\"responseStatus\": \"FAILURE\",\n"
                                    + "\t\"responseMessage\": \"Authentication failed for user dummyUser\",\n"
                                    + "\t\"errors\": [{\n" + "\t\t\"type\": \"USERNAME_OR_PASSWORD_INCORRECT\",\n"
                                    + "\t\t\"message\": \"Authentication failed for user: dummyUser.\"\n" + "\t}],\n"
                                    + "\t\"errorType\": \"AUTHENTICATION_FAILED\"\n" + "}";

        when(_browseContext.getConnectionProperties()).thenReturn(_connectionPropertiesUserCredentials);

        VaultApiHandler vaultApiHandler = VaultApiHandlerFactory.create(_browseContext);

        InputStream inputStream = new ByteArrayInputStream(authResponseString.getBytes(StandardCharsets.UTF_8));

        try (CloseableHttpClient client = mock(CloseableHttpClient.class)) {
            CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
            StatusLine statusLine = mock(StatusLine.class);

            when(statusLine.getStatusCode()).thenReturn(200);
            when(response.getStatusLine()).thenReturn(statusLine);
            when(response.getEntity().getContentLength()).thenReturn(1L);
            when(response.getEntity().getContent()).thenReturn(inputStream);
            when(client.execute(any(HttpPost.class))).thenReturn(response);

            Executable executable = () -> vaultApiHandler.requestSessionId(client);

            String errorMessage =
                    "Error obtaining Session ID. Response status:FAILURE. Response message:'Authentication failed for"
                    + " user dummyUser'. Error type:AUTHENTICATION_FAILED. Errors: "
                    + "USERNAME_OR_PASSWORD_INCORRECT ";

            assertEquals(errorMessage, getThrowableMessage(executable));
        }
    }

    @Test
    void requestSessionIdOAuth2WithExpiredAccessTokenTest() throws IOException {
        String oauth2ErrorResponse =
                "{\n" + "\t\"vaults\": [],\n" + "\t\"responseStatus\": \"FAILURE\",\n" + "\t\"errors\": [{\n"
                + "\t\t\"type\": \"EXPIRED_TOKEN\",\n"
                + "\t\t\"message\": \"Expired access token: expiredAccessToken\"\n" + "\t}]\n" + "}";

        when(_browseContext.getConnectionProperties()).thenReturn(_connectionPropertiesUserCredentials);

        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);

        InputStream errorInputStream = new ByteArrayInputStream(oauth2ErrorResponse.getBytes(StandardCharsets.UTF_8));
        CloseableHttpResponse errorResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(errorResponse.getStatusLine()).thenReturn(statusLine);
        when(errorResponse.getEntity().getContentLength()).thenReturn(1L);
        when(errorResponse.getEntity().getContent()).thenReturn(errorInputStream);

        InputStream successInputStream = new ByteArrayInputStream(
                _sessionResponseSuccess.getBytes(StandardCharsets.UTF_8));
        CloseableHttpResponse successResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(successResponse.getStatusLine()).thenReturn(statusLine);
        when(successResponse.getEntity().getContentLength()).thenReturn(1L);
        when(successResponse.getEntity().getContent()).thenReturn(successInputStream);

        CloseableHttpClient client = mock(CloseableHttpClient.class);
        when(client.execute(any(HttpPost.class))).thenReturn(errorResponse, successResponse);

        VaultApiHandler vaultApiHandler = VaultApiHandlerFactory.create(_browseContext);

        assertEquals("sessionIdValue", vaultApiHandler.requestSessionId(client));
    }

    @Test
    void requestSessionIdOAuth2WithInvalidAccessTokenTest() throws IOException {
        String oauth2ErrorResponse =
                "{\n" + "\t\"vaults\": [],\n" + "\t\"responseStatus\": \"FAILURE\",\n" + "\t\"errors\": [{\n"
                + "\t\t\"type\": \"INVALID_TOKEN\",\n"
                + "\t\t\"message\": \"Invalid access token: invalidAccessToken\"\n" + "\t}]\n" + "}";

        when(_browseContext.getConnectionProperties()).thenReturn(_connectionPropertiesUserCredentials);

        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);

        InputStream errorInputStream = new ByteArrayInputStream(oauth2ErrorResponse.getBytes(StandardCharsets.UTF_8));
        CloseableHttpResponse errorResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(errorResponse.getStatusLine()).thenReturn(statusLine);
        when(errorResponse.getEntity().getContentLength()).thenReturn(1L);
        when(errorResponse.getEntity().getContent()).thenReturn(errorInputStream);

        InputStream successInputStream = new ByteArrayInputStream(
                _sessionResponseSuccess.getBytes(StandardCharsets.UTF_8));
        CloseableHttpResponse successResponse = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        when(successResponse.getStatusLine()).thenReturn(statusLine);
        when(successResponse.getEntity().getContentLength()).thenReturn(1L);
        when(successResponse.getEntity().getContent()).thenReturn(successInputStream);

        CloseableHttpClient client = mock(CloseableHttpClient.class);
        when(client.execute(any(HttpPost.class))).thenReturn(errorResponse, successResponse);

        VaultApiHandler vaultApiHandler = VaultApiHandlerFactory.create(_browseContext);

        assertEquals("sessionIdValue", vaultApiHandler.requestSessionId(client));
    }

    @Test
    void createAuthRequestOAuthTest() throws IOException {
        OAuth2Context oAuth2Context = mock(OAuth2Context.class, RETURNS_DEEP_STUBS);
        _connectionPropertiesOAuth2.put("veevaOauth", oAuth2Context);

        when(_browseContext.getConnectionProperties()).thenReturn(_connectionPropertiesOAuth2);
        when(oAuth2Context.getOAuth2Token(false).getAccessToken()).thenReturn("accessToken");

        VaultApiHandler vaultApiHandler = VaultApiHandlerFactory.create(_browseContext);
        HttpPost request = vaultApiHandler.createRequest(false);

        assertEquals("https://login.veevavault.com/auth/oauth/session/_profile-id", request.getURI().toString());
        assertEquals("Bearer accessToken", request.getFirstHeader("Authorization").getValue());
        assertEquals("Boomi", request.getFirstHeader("X-VaultAPI-ClientID").getValue());
    }
}
