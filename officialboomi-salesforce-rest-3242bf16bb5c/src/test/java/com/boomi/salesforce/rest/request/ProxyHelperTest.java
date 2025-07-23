// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.ConnectorContext;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.Credentials;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class ProxyHelperTest {

    @Test
    void proxyDisabled() {
        ConnectorContext context = Mockito.mock(ConnectorContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getConfig().getProxyConfig().isProxyEnabled()).thenReturn(false);
        HttpClientBuilder builder = Mockito.spy(HttpClientBuilder.create());

        ProxyHelper helper = new ProxyHelper(context);
        helper.configure(builder);

        Mockito.verify(builder, Mockito.never()).setProxy(Mockito.any());
        Mockito.verify(builder, Mockito.never()).setDefaultCredentialsProvider(Mockito.any());
    }

    @Test
    void proxyEnabledWithoutAuthentication() {
        ConnectorContext context = Mockito.mock(ConnectorContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getConfig().getProxyConfig().isProxyEnabled()).thenReturn(true);
        when(context.getConfig().getProxyConfig().isAuthenticationEnabled()).thenReturn(false);
        when(context.getConfig().getProxyConfig().getProxyHost()).thenReturn("https://example.com");
        when(context.getConfig().getProxyConfig().getProxyPort()).thenReturn("80");

        HttpClientBuilder builder = Mockito.spy(HttpClientBuilder.create());
        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(builder.setProxy(hostCaptor.capture())).thenReturn(builder);

        ProxyHelper helper = new ProxyHelper(context);
        helper.configure(builder);

        Mockito.verify(builder, times(1)).setProxy(Mockito.any());
        Assertions.assertEquals(80, hostCaptor.getValue().getPort());
        Assertions.assertEquals("https://example.com", hostCaptor.getValue().getHostName());

        Mockito.verify(builder, Mockito.never()).setDefaultCredentialsProvider(Mockito.any());
    }

    @Test
    void proxyEnabledWithAuthenticationEnabled() {
        ConnectorContext context = Mockito.mock(ConnectorContext.class, Mockito.RETURNS_DEEP_STUBS);
        when(context.getConfig().getProxyConfig().isProxyEnabled()).thenReturn(true);
        when(context.getConfig().getProxyConfig().isAuthenticationEnabled()).thenReturn(true);
        when(context.getConfig().getProxyConfig().getProxyHost()).thenReturn("https://example.com");
        when(context.getConfig().getProxyConfig().getProxyPort()).thenReturn("80");
        when(context.getConfig().getProxyConfig().getProxyUser()).thenReturn("username");
        when(context.getConfig().getProxyConfig().getProxyPassword()).thenReturn("password");

        HttpClientBuilder builder = Mockito.spy(HttpClientBuilder.create());
        ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
        when(builder.setProxy(hostCaptor.capture())).thenReturn(builder);
        ArgumentCaptor<CredentialsProvider> credentialsCaptor = ArgumentCaptor.forClass(CredentialsProvider.class);
        when(builder.setDefaultCredentialsProvider(credentialsCaptor.capture())).thenReturn(builder);

        ProxyHelper helper = new ProxyHelper(context);
        helper.configure(builder);

        Mockito.verify(builder, times(1)).setProxy(Mockito.any());
        Assertions.assertEquals(80, hostCaptor.getValue().getPort());
        Assertions.assertEquals("https://example.com", hostCaptor.getValue().getHostName());

        Mockito.verify(builder, times(1)).setDefaultCredentialsProvider(Mockito.any());
        Credentials credentials = credentialsCaptor.getValue().getCredentials(new AuthScope(hostCaptor.getValue()),
                null);
        Assertions.assertEquals("username", credentials.getUserPrincipal().getName());
        Assertions.assertArrayEquals("password".toCharArray(), credentials.getPassword());
    }
}
