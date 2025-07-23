// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.TestUtil;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.net.URIAuthority;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;

public class AuthenticationRequestExecutorTest {

    @BeforeAll
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    @Test
    void executeAuthenticateSuccessfully() throws IOException {
        final int successStatus = 200;

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        ArgumentCaptor<ClassicHttpRequest> requestCaptor = ArgumentCaptor.forClass(ClassicHttpRequest.class);
        ArgumentCaptor<HttpContext> contextCaptor = ArgumentCaptor.forClass(HttpContext.class);

        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(
                        client.executeOpen(hostCaptor.capture(), requestCaptor.capture(), contextCaptor.capture()).getCode())
                .thenReturn(successStatus);

        ClassicHttpRequest request = Mockito.mock(ClassicHttpRequest.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getAuthority()).thenReturn(new URIAuthority("example.com", 80));

        AuthenticationRequestExecutor executor = new AuthenticationRequestExecutor(client);

        ClassicHttpResponse response = executor.executeAuthenticate(request);

        Assertions.assertNotNull(response);
        Assertions.assertEquals(request, requestCaptor.getValue());
        Assertions.assertNull(contextCaptor.getValue());
        Assertions.assertEquals(new HttpHost("http", "example.com", 80), hostCaptor.getValue());
    }

    @Test
    void executeAuthenticateWithErrors() throws IOException {
        final int errorStatus = 401;

        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        ArgumentCaptor<ClassicHttpRequest> requestCaptor = ArgumentCaptor.forClass(ClassicHttpRequest.class);
        ArgumentCaptor<HttpContext> contextCaptor = ArgumentCaptor.forClass(HttpContext.class);

        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(
                        client.executeOpen(hostCaptor.capture(), requestCaptor.capture(), contextCaptor.capture()).getCode())
                .thenReturn(errorStatus);

        ClassicHttpRequest request = Mockito.mock(ClassicHttpRequest.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(request.getScheme()).thenReturn("http");
        Mockito.when(request.getAuthority()).thenReturn(new URIAuthority("example.com", 80));

        AuthenticationRequestExecutor executor = new AuthenticationRequestExecutor(client);

        Assertions.assertThrowsExactly(ConnectorException.class, () -> executor.executeAuthenticate(request));

        Assertions.assertEquals(request, requestCaptor.getValue());
        Assertions.assertNull(contextCaptor.getValue());
        Assertions.assertEquals(new HttpHost("http", "example.com", 80), hostCaptor.getValue());
    }
}
