// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.util;

import com.boomi.connector.api.AtomProxyConfig;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.util.IOUtil;

import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class HttpClientFactoryTest {

    @Test
    void createHttpClientTest() {
        ConnectorContext context = mock(ConnectorContext.class, RETURNS_DEEP_STUBS);
        AtomProxyConfig config = mock(AtomProxyConfig.class, Mockito.RETURNS_DEEP_STUBS);
        when(config.getProxyHost()).thenReturn("proxy.host.com");
        when(config.getProxyPort()).thenReturn("80");
        when(config.getProxyUser()).thenReturn("username");
        when(config.getProxyPassword()).thenReturn("password");
        when(config.isProxyEnabled()).thenReturn(true);
        when(config.isAuthenticationEnabled()).thenReturn(true);
        when(context.getConfig().getProxyConfig()).thenReturn(config);

        CloseableHttpClient client = HttpClientFactory.createHttpClient(context);

        Mockito.verify(context, times(4)).getConfig();
        Mockito.verify(config, times(1)).isProxyEnabled();
        Mockito.verify(config, times(1)).isAuthenticationEnabled();
        Mockito.verify(config, times(2)).getProxyHost();
        Mockito.verify(config, times(2)).getProxyPort();
        Mockito.verify(config, times(1)).getProxyUser();
        Mockito.verify(config, times(1)).getProxyPassword();

        IOUtil.closeQuietly(client);
    }
}
