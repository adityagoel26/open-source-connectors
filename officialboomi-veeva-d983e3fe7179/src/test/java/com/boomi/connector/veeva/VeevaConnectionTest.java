// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva;

import com.boomi.common.apache.http.entity.CloseableInputStreamEntity;
import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.util.ConnectorCache;
import com.boomi.connector.util.ConnectorCacheFactory;
import com.boomi.connector.veeva.auth.cache.SessionCache;
import com.boomi.connector.veeva.retry.VeevaResponseUtil;
import com.boomi.connector.veeva.util.ExecutionUtils;
import com.boomi.connector.veeva.vault.api.VaultApiHandler;
import com.boomi.util.StreamUtil;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class VeevaConnectionTest {

    private static final String FAILURE_RESPONSE = "{\n" + "\t\"responseStatus\": \"FAILURE\",\n" + "\t\"errors\": [{\n"
            + "\t\t\"type\": \"INVALID_SESSION_ID\",\n" + "\t\t\"message\": \"Invalid or expired session ID.\"\n"
            + "\t}]\n" + "}";

    private PropertyMap _connectionProperties;

    @BeforeEach
    void setup() {
        _connectionProperties = new MutablePropertyMap();
        _connectionProperties.put("authenticationType", "USER_CREDENTIALS");
        _connectionProperties.put("vaultSubdomain", "subdomain.veeva.com");
        _connectionProperties.put("apiVersion", "23.3");
        _connectionProperties.put("username", "the username");
        _connectionProperties.put("password", "the password");
        _connectionProperties.put("sessionTimeout", 10L);
    }

    @Test
    void getYamlSpecTest() {
        BrowseContext browseContext = mock(BrowseContext.class);
        when(browseContext.getConnectionProperties()).thenReturn(_connectionProperties);
        when(browseContext.getOperationProperties()).thenReturn(new MutablePropertyMap());

        VeevaConnection connection = new VeevaConnection(browseContext);

        assertEquals("openapi.yaml", connection.getSpec());
    }

    @Test
    void doExecuteTest() throws IOException {
        BrowseContext browseContext = mock(BrowseContext.class);
        VaultApiHandler vaultApiHandler = mock(VaultApiHandler.class);
        CloseableHttpClient clientMock = mock(CloseableHttpClient.class);
        CloseableHttpResponse responseMock = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        SessionCache sessionCache = new SessionCache("key", "sessionId", 10L);
        CloseableInputStreamEntity entity = new CloseableInputStreamEntity(
                new ByteArrayInputStream("response".getBytes(StandardCharsets.UTF_8)), -1L,
                ContentType.APPLICATION_JSON);
        Header[] headers = { new BasicHeader("Content-type", "contentTypeValue") };
        Header successfulResponseHeader = new BasicHeader("X-VaultAPI-Status", "SUCCESS");

        when(browseContext.getOperationProperties()).thenReturn(new MutablePropertyMap());
        when(browseContext.getConnectionProperties()).thenReturn(_connectionProperties);
        when(responseMock.getEntity()).thenReturn(entity);
        when(responseMock.getStatusLine().getStatusCode()).thenReturn(200);
        when(responseMock.getAllHeaders()).thenReturn(headers);
        when(responseMock.getFirstHeader("X-VaultAPI-Status")).thenReturn(successfulResponseHeader);
        when(clientMock.execute(any(HttpUriRequest.class))).thenReturn(responseMock);
        when(vaultApiHandler.getSessionIdFromCache(browseContext)).thenReturn("id");

        try (MockedStatic<VeevaResponseUtil> responseUtil = mockStatic(VeevaResponseUtil.class);
                MockedStatic<ConnectorCache> connectorCache = mockStatic(ConnectorCache.class)) {
            responseUtil.when(() -> VeevaResponseUtil.getWithResettableContentStream(any(CloseableHttpResponse.class)))
                    .thenReturn(responseMock);
            connectorCache.when(() -> ConnectorCache.getCache(anyString(), any(ConnectorContext.class),
                    any(ConnectorCacheFactory.class))).thenReturn(sessionCache);

            VeevaConnection connection = new VeevaConnection(browseContext);
            CloseableHttpResponse response = ExecutionUtils.execute("GET", "/metadata/vobjects/product__v", clientMock,
                    null, connection, new SimpleTrackedData(1, null));
            assertEquals(response, responseMock);
        }
    }

    @Test
    void doExecuteFailureTest() throws IOException {
        BrowseContext browseContext = mock(BrowseContext.class);
        VaultApiHandler vaultApiHandler = mock(VaultApiHandler.class);
        CloseableHttpClient clientMock = mock(CloseableHttpClient.class);
        CloseableHttpResponse responseMock = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        Header header = new BasicHeader("Content-type", "application/json");
        HttpEntity entity = new CloseableInputStreamEntity(
                new ByteArrayInputStream(FAILURE_RESPONSE.getBytes(StandardCharsets.UTF_8)), -1L,
                ContentType.APPLICATION_JSON);
        HttpEntity resettableEntity = new InputStreamEntity(StreamUtil.tempCopy(entity.getContent()));

        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("authenticationType", "USER_CREDENTIALS");
        connectionProperties.put("vaultSubdomain", "www.veeva.com");
        connectionProperties.put("apiVersion", "v22.3");
        connectionProperties.put("username", "the username");
        connectionProperties.put("password", "the password");
        connectionProperties.put("sessionTimeout", 10L);

        ConcurrentMap<Object, Object> cache = new ConcurrentHashMap<>();
        String cacheKey = "www.veeva.com,the username,10";
        cache.put(cacheKey, new SessionCache(cacheKey, "sessionId", 10L));

        when(browseContext.getOperationProperties()).thenReturn(new MutablePropertyMap());
        when(browseContext.getConnectionProperties()).thenReturn(connectionProperties, connectionProperties,
                connectionProperties, connectionProperties, connectionProperties, connectionProperties);
        when(browseContext.getConnectorCache()).thenReturn(cache);
        when(responseMock.getFirstHeader("X-VaultAPI-Status")).thenReturn(null);
        when(responseMock.getFirstHeader("Content-Type")).thenReturn(header, header);
        when(responseMock.getEntity()).thenReturn(entity, entity, entity, resettableEntity);
        when(responseMock.getStatusLine().getStatusCode()).thenReturn(200);
        when(clientMock.execute(any(HttpUriRequest.class))).thenReturn(responseMock);
        when(vaultApiHandler.getSessionIdFromCache(browseContext)).thenReturn("id");
        when(vaultApiHandler.getSessionCacheKey()).thenReturn(cacheKey);

        VeevaConnection connection = new VeevaConnection(browseContext, vaultApiHandler);
        CloseableHttpResponse response = ExecutionUtils.execute("GET", "/metadata/vobjects/product__v", clientMock,
                null, connection, new SimpleTrackedData(1, null));
        Mockito.verify(response, times(1)).close();
        assertEquals(0, cache.size());
    }

    @Test
    void connectionTest() throws IOException {
        BrowseContext browseContext = mock(BrowseContext.class, RETURNS_DEEP_STUBS);
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        VaultApiHandler vaultApiHandler = mock(VaultApiHandler.class);

        ArgumentCaptor<HttpRequestBase> requestCaptor = ArgumentCaptor.forClass(HttpRequestBase.class);

        when(vaultApiHandler.requestSessionId(any(CloseableHttpClient.class))).thenReturn("id");
        when(vaultApiHandler.getVaultApiEndpoint()).thenReturn(new URL("https://veeva.vault.com/api/v23.3/"));
        when(client.execute(requestCaptor.capture())).thenReturn(response);
        when(browseContext.getOperationProperties()).thenReturn(new MutablePropertyMap());
        when(browseContext.getConnectionProperties().getProperty("authenticationType", "USER_CREDENTIALS")).thenReturn(
                "USER_CREDENTIALS");
        when(browseContext.getConnectionProperties().getProperty("vaultSubdomain")).thenReturn("sub.veeva.com");
        when(browseContext.getConnectionProperties().getProperty("apiVersion")).thenReturn("23.3");
        when(browseContext.getConnectionProperties().getProperty("username")).thenReturn("user");
        when(browseContext.getConnectionProperties().getProperty("password")).thenReturn("pass");
        when(browseContext.getConnectionProperties().getLongProperty("sessionTimeout", 10L)).thenReturn(10L);

        new VeevaConnection(browseContext, vaultApiHandler).testConnection(client, "/product");

        assertEquals("https://veeva.vault.com/api/v23.3/product", requestCaptor.getValue().getURI().toString());
        assertEquals("id", requestCaptor.getValue().getFirstHeader("Authorization").getValue());
    }

    @Test
    void doExecuteSetClientIDHeaderTest() throws IOException {
        BrowseContext browseContext = mock(BrowseContext.class);
        VaultApiHandler vaultApiHandler = mock(VaultApiHandler.class);
        CloseableHttpClient clientMock = mock(CloseableHttpClient.class);
        CloseableHttpResponse responseMock = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        SessionCache sessionCache = new SessionCache("key", "sessionId", 10L);
        CloseableInputStreamEntity entity = new CloseableInputStreamEntity(
                new ByteArrayInputStream("response".getBytes(StandardCharsets.UTF_8)), -1L,
                ContentType.APPLICATION_JSON);
        Header[] headers = { new BasicHeader("Content-type", "contentTypeValue") };
        Header successfulResponseHeader = new BasicHeader("X-VaultAPI-Status", "SUCCESS");

        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("vaultSubdomain", "subdomain.veeva.com");
        connectionProperties.put("apiVersion", "v23.3");
        connectionProperties.put("username", "user");
        connectionProperties.put("password", "password");
        connectionProperties.put("veevaClientID", "expected_ID");
        when(browseContext.getConnectionProperties()).thenReturn(connectionProperties);
        when(browseContext.getOperationProperties()).thenReturn(new MutablePropertyMap());
        when(responseMock.getEntity()).thenReturn(entity);
        when(responseMock.getStatusLine().getStatusCode()).thenReturn(200);
        when(responseMock.getAllHeaders()).thenReturn(headers);
        when(responseMock.getFirstHeader("X-VaultAPI-Status")).thenReturn(successfulResponseHeader);
        when(vaultApiHandler.getSessionIdFromCache(browseContext)).thenReturn("id");

        ArgumentCaptor<HttpUriRequest> requestCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
        when(clientMock.execute(requestCaptor.capture())).thenReturn(responseMock);

        try (MockedStatic<VeevaResponseUtil> responseUtil = mockStatic(VeevaResponseUtil.class);
                MockedStatic<ConnectorCache> connectorCache = mockStatic(ConnectorCache.class)) {
            responseUtil.when(() -> VeevaResponseUtil.getWithResettableContentStream(any(CloseableHttpResponse.class)))
                    .thenReturn(responseMock);
            connectorCache.when(() -> ConnectorCache.getCache(anyString(), any(ConnectorContext.class),
                    any(ConnectorCacheFactory.class))).thenReturn(sessionCache);

            VeevaConnection connection = new VeevaConnection(browseContext);
            CloseableHttpResponse response = ExecutionUtils.execute("GET", "/metadata/vobjects/product__v", clientMock,
                    null, connection, new SimpleTrackedData(1, null));
            assertEquals(response, responseMock);

            HttpUriRequest request = requestCaptor.getValue();
            Header clientIDHeader = request.getFirstHeader("X-VaultAPI-ClientID");
            assertEquals("Boomi_expected_ID", clientIDHeader.getValue());
        }
    }
}
