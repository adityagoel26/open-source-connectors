// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.authenticator.TokenManager;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.util.StringUtil;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.net.URIAuthority;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class RequestExecutorTest {

    private static final BasicHeader XML_CONTENT_HEADER = new BasicHeader("Content-Type", "application/xml");

    @Test
    void executeSuccess() throws IOException, URISyntaxException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        ConnectionProperties connectionProps = mock(ConnectionProperties.class);

        RequestExecutor executor = new RequestExecutor(client, null, connectionProps, null);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(response.getCode()).thenReturn(200);

        ClassicHttpRequest request = mock(ClassicHttpRequest.class, RETURNS_DEEP_STUBS);
        when(request.getScheme()).thenReturn("https");
        when(request.getAuthority()).thenReturn(new URIAuthority("example.com"));
        when(request.getUri().toString()).thenReturn("https://example.com");
        when(request.getMethod()).thenReturn("GET");

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(client.executeOpen(hostCaptor.capture(), eq(request), eq(null))).thenReturn(response);

        ClassicHttpResponse result = executor.execute(request);

        assertEquals(response, result);
        assertEquals("example.com", hostCaptor.getValue().getHostName());
        assertEquals("https", hostCaptor.getValue().getSchemeName());

        verify(client, times(1)).executeOpen(any(), any(), any());
        verify(response, never()).close();
        verify(request.getEntity(), atLeast(1)).close();
        verify(connectionProps, times(1)).logInfo("Executing request: [GET] https://example.com");
    }

    @Test
    void executeUnauthorizedRequest() throws IOException, ProtocolException, URISyntaxException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        TokenManager tokenManager = mock(TokenManager.class, RETURNS_DEEP_STUBS);
        ConnectionProperties connectionProperties = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);
        OperationProperties operationProperties = mock(OperationProperties.class, RETURNS_DEEP_STUBS);

        RequestExecutor executor = new RequestExecutor(client, tokenManager, connectionProperties, operationProperties);

        ClassicHttpResponse unauthorizedResponse = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(unauthorizedResponse.getCode()).thenReturn(401);
        when(unauthorizedResponse.getHeader("Content-Type")).thenReturn(XML_CONTENT_HEADER);
        when(unauthorizedResponse.getEntity().getContent()).thenReturn(
                new ByteArrayInputStream("<message>error</message>".getBytes(StringUtil.UTF8_CHARSET)));

        ClassicHttpResponse successResponse = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(successResponse.getCode()).thenReturn(200);

        ClassicHttpRequest request = mock(ClassicHttpRequest.class, RETURNS_DEEP_STUBS);
        when(request.getScheme()).thenReturn("https");
        when(request.getAuthority()).thenReturn(new URIAuthority("example.com"));
        when(request.getUri().toString()).thenReturn("https://example.com");
        when(request.getMethod()).thenReturn("PUT");

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(client.executeOpen(hostCaptor.capture(), eq(request), eq(null))).thenReturn(unauthorizedResponse,
                successResponse);

        ClassicHttpResponse result = executor.execute(request);

        assertEquals(successResponse, result);
        assertEquals("example.com", hostCaptor.getValue().getHostName());
        assertEquals("https", hostCaptor.getValue().getSchemeName());

        verify(client, times(2)).executeOpen(any(), any(), any());
        verify(unauthorizedResponse, atLeast(1)).close();
        verify(successResponse, never()).close();
        verify(request.getEntity(), atLeast(1)).close();
        verify(connectionProperties, times(2)).logInfo("Executing request: [PUT] https://example.com");
    }

    @Test
    void executeFailedRequest() throws IOException, ProtocolException, URISyntaxException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        TokenManager tokenManager = mock(TokenManager.class, RETURNS_DEEP_STUBS);
        ConnectionProperties connectionProperties = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);
        OperationProperties operationProperties = mock(OperationProperties.class, RETURNS_DEEP_STUBS);

        RequestExecutor executor = new RequestExecutor(client, tokenManager, connectionProperties, operationProperties);

        ClassicHttpResponse failedResponse = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(failedResponse.getCode()).thenReturn(500);
        when(failedResponse.getHeader("Content-Type")).thenReturn(XML_CONTENT_HEADER);
        when(failedResponse.getEntity().getContent()).thenReturn(
                new ByteArrayInputStream("<message>error</message>".getBytes(StringUtil.UTF8_CHARSET)));

        ClassicHttpRequest request = mock(ClassicHttpRequest.class, RETURNS_DEEP_STUBS);
        when(request.getScheme()).thenReturn("https");
        when(request.getAuthority()).thenReturn(new URIAuthority("example.com"));
        when(request.getUri().toString()).thenReturn("https://example.com");
        when(request.getMethod()).thenReturn("POST");

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(client.executeOpen(hostCaptor.capture(), eq(request), eq(null))).thenReturn(failedResponse);

        assertThrowsExactly(ConnectorException.class, () -> executor.execute(request));

        assertEquals("example.com", hostCaptor.getValue().getHostName());
        assertEquals("https", hostCaptor.getValue().getSchemeName());

        verify(client, times(1)).executeOpen(any(), any(), any());
        verify(failedResponse, atLeast(1)).close();
        verify(request.getEntity(), atLeast(1)).close();
        verify(connectionProperties, times(1)).logInfo("Executing request: [POST] https://example.com");
    }

    @Test
    void executeStreamSuccessfully() throws IOException, ProtocolException, URISyntaxException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        TokenManager tokenManager = mock(TokenManager.class, RETURNS_DEEP_STUBS);
        ConnectionProperties connectionProperties = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);
        OperationProperties operationProperties = mock(OperationProperties.class, RETURNS_DEEP_STUBS);

        ArgumentCaptor<String> apiLimitCaptor = ArgumentCaptor.forClass(String.class);
        doNothing().when(operationProperties).setAPILimit(apiLimitCaptor.capture());

        RequestExecutor executor = new RequestExecutor(client, tokenManager, connectionProperties, operationProperties);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(response.getCode()).thenReturn(200);
        when(response.getHeader("Sforce-Limit-Info")).thenReturn(new BasicHeader("Sforce-Limit-Info", "42"));

        ClassicHttpRequest request = mock(ClassicHttpRequest.class, RETURNS_DEEP_STUBS);
        when(request.getScheme()).thenReturn("https");
        when(request.getAuthority()).thenReturn(new URIAuthority("example.com"));
        when(request.getUri().toString()).thenReturn("https://example.com");
        when(request.getMethod()).thenReturn("PATCH");

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(client.executeOpen(hostCaptor.capture(), eq(request), eq(null))).thenReturn(response);

        ClassicHttpResponse result = executor.executeStream(request, "errorMessage");

        assertEquals(response, result);
        assertEquals("example.com", hostCaptor.getValue().getHostName());
        assertEquals("https", hostCaptor.getValue().getSchemeName());
        assertEquals("Sforce-Limit-Info: 42", apiLimitCaptor.getValue());

        verify(client, times(1)).executeOpen(any(), any(), any());
        verify(response, never()).close();
        verify(request.getEntity(), atLeast(1)).close();
        verify(connectionProperties, times(1)).logInfo("Executing request: [PATCH] https://example.com");
    }

    @Test
    void executeStreamWithFailedResponse() throws IOException, ProtocolException, URISyntaxException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        TokenManager tokenManager = mock(TokenManager.class, RETURNS_DEEP_STUBS);
        ConnectionProperties connectionProperties = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);
        OperationProperties operationProperties = mock(OperationProperties.class, RETURNS_DEEP_STUBS);

        RequestExecutor executor = new RequestExecutor(client, tokenManager, connectionProperties, operationProperties);

        ClassicHttpResponse failedResponse = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(failedResponse.getCode()).thenReturn(500);
        when(failedResponse.getHeader("Sforce-Limit-Info")).thenReturn(new BasicHeader("Sforce-Limit-Info", "42"));
        when(failedResponse.getHeader("Content-Type")).thenReturn(XML_CONTENT_HEADER);
        when(failedResponse.getEntity().getContent()).thenReturn(
                new ByteArrayInputStream("<message>error</message>".getBytes(StringUtil.UTF8_CHARSET)));

        ClassicHttpRequest request = mock(ClassicHttpRequest.class, RETURNS_DEEP_STUBS);
        when(request.getScheme()).thenReturn("https");
        when(request.getAuthority()).thenReturn(new URIAuthority("example.com"));
        when(request.getUri().toString()).thenReturn("https://example.com");
        when(request.getMethod()).thenReturn("DELETE");

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(client.executeOpen(hostCaptor.capture(), eq(request), eq(null))).thenReturn(failedResponse);

        assertThrowsExactly(ConnectorException.class, () -> executor.executeStream(request, "errorMessage"));

        assertEquals("example.com", hostCaptor.getValue().getHostName());
        assertEquals("https", hostCaptor.getValue().getSchemeName());

        verify(client, times(1)).executeOpen(any(), any(), any());
        verify(failedResponse, atLeast(1)).close();
        verify(request.getEntity(), atLeast(1)).close();
        verify(connectionProperties, times(1)).logInfo("Executing request: [DELETE] https://example.com");
    }

    @Test
    void executeVoidSuccessfully() throws IOException, ProtocolException, URISyntaxException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        TokenManager tokenManager = mock(TokenManager.class, RETURNS_DEEP_STUBS);
        ConnectionProperties connectionProperties = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);
        OperationProperties operationProperties = mock(OperationProperties.class, RETURNS_DEEP_STUBS);

        ArgumentCaptor<String> apiLimitCaptor = ArgumentCaptor.forClass(String.class);
        doNothing().when(operationProperties).setAPILimit(apiLimitCaptor.capture());

        RequestExecutor executor = new RequestExecutor(client, tokenManager, connectionProperties, operationProperties);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(response.getCode()).thenReturn(200);
        when(response.getHeader("Sforce-Limit-Info")).thenReturn(new BasicHeader("Sforce-Limit-Info", "42"));

        ClassicHttpRequest request = mock(ClassicHttpRequest.class, RETURNS_DEEP_STUBS);
        when(request.getScheme()).thenReturn("https");
        when(request.getAuthority()).thenReturn(new URIAuthority("example.com"));
        when(request.getUri().toString()).thenReturn("https://example.com");
        when(request.getMethod()).thenReturn("PUT");

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(client.executeOpen(hostCaptor.capture(), eq(request), eq(null))).thenReturn(response);

        executor.executeVoid(request, "errorMessage");

        assertEquals("example.com", hostCaptor.getValue().getHostName());
        assertEquals("https", hostCaptor.getValue().getSchemeName());
        assertEquals("Sforce-Limit-Info: 42", apiLimitCaptor.getValue());

        verify(client, times(1)).executeOpen(any(), any(), any());
        verify(response, atLeast(1)).close();
        verify(request.getEntity(), atLeast(1)).close();
        verify(connectionProperties, times(1)).logInfo("Executing request: [PUT] https://example.com");
    }

    @Test
    void executeVoidWithFailedResponse() throws IOException, ProtocolException, URISyntaxException {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        TokenManager tokenManager = mock(TokenManager.class, RETURNS_DEEP_STUBS);
        ConnectionProperties connectionProperties = mock(ConnectionProperties.class, RETURNS_DEEP_STUBS);
        OperationProperties operationProperties = mock(OperationProperties.class, RETURNS_DEEP_STUBS);

        RequestExecutor executor = new RequestExecutor(client, tokenManager, connectionProperties, operationProperties);

        ClassicHttpResponse failedResponse = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        when(failedResponse.getCode()).thenReturn(500);
        when(failedResponse.getHeader("Sforce-Limit-Info")).thenReturn(new BasicHeader("Sforce-Limit-Info", "42"));
        when(failedResponse.getHeader("Content-Type")).thenReturn(XML_CONTENT_HEADER);
        when(failedResponse.getEntity().getContent()).thenReturn(
                new ByteArrayInputStream("<message>error</message>".getBytes(StringUtil.UTF8_CHARSET)));

        ClassicHttpRequest request = mock(ClassicHttpRequest.class, RETURNS_DEEP_STUBS);
        when(request.getScheme()).thenReturn("https");
        when(request.getAuthority()).thenReturn(new URIAuthority("example.com"));
        when(request.getUri().toString()).thenReturn("https://example.com");
        when(request.getMethod()).thenReturn("POST");

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(client.executeOpen(hostCaptor.capture(), eq(request), eq(null))).thenReturn(failedResponse);

        assertThrowsExactly(ConnectorException.class, () -> executor.executeVoid(request, "errorMessage"));

        assertEquals("example.com", hostCaptor.getValue().getHostName());
        assertEquals("https", hostCaptor.getValue().getSchemeName());

        verify(client, times(1)).executeOpen(any(), any(), any());
        verify(failedResponse, atLeast(1)).close();
        verify(request.getEntity(), atLeast(1)).close();
        verify(connectionProperties, times(1)).logInfo("Executing request: [POST] https://example.com");
    }
}
