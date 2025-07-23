// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.retry;

import com.boomi.common.apache.http.entity.CloseableInputStreamEntity;
import com.boomi.common.apache.http.response.HttpResult;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.veeva.VeevaOperationConnection;
import com.boomi.connector.veeva.auth.cache.SessionCache;
import com.boomi.util.StreamUtil;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RetryableClientTest {

    @Test
    void executeTest() throws IOException {
        OperationContext contextMock = mock(OperationContext.class);
        VeevaOperationConnection connectionMock = mock(VeevaOperationConnection.class);
        CloseableHttpClient clientMock = mock(CloseableHttpClient.class);
        HttpContext httpContextMock = mock(HttpContext.class);
        CloseableHttpResponse responseMock = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        RequestBuilder builderMock = mock(RequestBuilder.class, RETURNS_DEEP_STUBS);
        CloseableInputStreamEntity entity = new CloseableInputStreamEntity(
                new ByteArrayInputStream("response".getBytes(StandardCharsets.UTF_8)), -1L,
                ContentType.APPLICATION_JSON);
        Header[] headers = { new BasicHeader("Content-type", "contentTypeValue") };
        Header successfulResponseHeader = new BasicHeader("X-VaultAPI-Status", "SUCCESS");

        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("authenticationType", "USER_CREDENTIALS");
        connectionProperties.put("vaultSubdomain", "vault.veeva.com");
        connectionProperties.put("apiVersion", "v23.3");
        connectionProperties.put("username", "username");
        connectionProperties.put("password", "password");
        connectionProperties.put("sessionTimeout", 10L);

        ConcurrentMap<Object, Object> cache = new ConcurrentHashMap<>();
        String cacheKey = "vault.veeva.com,username,10";
        cache.put(cacheKey, new SessionCache(cacheKey, "sessionId", 10L));

        when(contextMock.getConnectionProperties()).thenReturn(connectionProperties);
        when(contextMock.getOperationProperties()).thenReturn(new MutablePropertyMap());
        when(contextMock.getConnectorCache()).thenReturn(cache);
        when(clientMock.execute(any(HttpUriRequest.class), any(HttpContext.class))).thenReturn(responseMock);
        when(contextMock.getConnectionProperties()).thenReturn(connectionProperties);
        when(connectionMock.getHttpClient()).thenReturn(clientMock);
        when(connectionMock.getContext()).thenReturn(contextMock);
        when(connectionMock.getHttpMethod()).thenReturn("GET");
        when(connectionMock.getHttpContext()).thenReturn(httpContextMock);
        when(responseMock.getEntity()).thenReturn(entity);
        when(responseMock.getStatusLine().getStatusCode()).thenReturn(200);
        when(responseMock.getAllHeaders()).thenReturn(headers);
        when(responseMock.getFirstHeader("X-VaultAPI-Status")).thenReturn(successfulResponseHeader);
        when(contextMock.createMetadata()).thenReturn(new SimplePayloadMetadata());

        try (RetryableClient client = new RetryableClient(connectionMock, new UnauthorizedRetryStrategy());
                HttpResult result = client.execute(builderMock).iterator().next()) {
            String response = StreamUtil.toString(result.getPayloads().iterator().next().readFrom(),
                    StandardCharsets.UTF_8);
            Assertions.assertEquals("response", response);
        }
    }
}
