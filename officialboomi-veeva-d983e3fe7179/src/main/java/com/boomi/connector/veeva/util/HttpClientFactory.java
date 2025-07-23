// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.util;

import com.boomi.common.apache.http.impl.CustomProxyRoutePlanner;
import com.boomi.connector.api.AtomProxyConfig;
import com.boomi.connector.api.ConnectorContext;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Utility class to build {@link HttpClient}s configured with proxy settings, provided those are available.
 */

public class HttpClientFactory {

    private HttpClientFactory() {
        throw new IllegalStateException("Utility class");
    }

    public static CloseableHttpClient createHttpClient(ConnectorContext context) {
        HttpClientBuilder builder = HttpClientBuilder.create().disableAutomaticRetries();
        AtomProxyConfig proxyConfig = getProxyConfig(context);

        if ((proxyConfig != null) && proxyConfig.isProxyEnabled()) {
            builder.setRoutePlanner(new CustomProxyRoutePlanner(proxyConfig));

            if (proxyConfig.isAuthenticationEnabled()) {
                builder.setDefaultCredentialsProvider(getProxyCredentialsProvider(proxyConfig));
            }
        }

        return builder.build();
    }

    private static AtomProxyConfig getProxyConfig(ConnectorContext context) {
        if (context != null && context.getConfig() != null && context.getConfig().getProxyConfig() != null) {
            return context.getConfig().getProxyConfig();
        }

        return null;
    }

    private static CredentialsProvider getProxyCredentialsProvider(AtomProxyConfig proxyConfig) {
        AuthScope proxyAuthScope = new AuthScope(proxyConfig.getProxyHost(),
                Integer.parseInt(proxyConfig.getProxyPort()));

        Credentials proxyCredentials = new UsernamePasswordCredentials(proxyConfig.getProxyUser(),
                proxyConfig.getProxyPassword());

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(proxyAuthScope, proxyCredentials);

        return credentialsProvider;
    }
}
