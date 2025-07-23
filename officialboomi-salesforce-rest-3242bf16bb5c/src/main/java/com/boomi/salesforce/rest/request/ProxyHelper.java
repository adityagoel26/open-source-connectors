// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.AtomProxyConfig;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.util.StringUtil;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.Credentials;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpHost;

/**
 * Utility class used to configure the proxy settings in the given {@link HttpClientBuilder}
 */
public class ProxyHelper {

    private final ConnectorContext _context;

    public ProxyHelper(ConnectorContext context) {
        _context = context;
    }

    /**
     * Set proxy settings in the given  {@link HttpClientBuilder}
     *
     * @param builder the client builder
     * @return the builder containing the proxy settings
     */
    public HttpClientBuilder configure(HttpClientBuilder builder) {
        AtomProxyConfig proxyConfig = _context.getConfig().getProxyConfig();
        if (!proxyConfig.isProxyEnabled()) {
            return builder;
        }

        HttpHost proxyHost = StringUtil.isBlank(proxyConfig.getProxyPort()) ? new HttpHost(proxyConfig.getProxyHost())
                : new HttpHost(proxyConfig.getProxyHost(), Integer.parseInt(proxyConfig.getProxyPort()));
        builder.setProxy(proxyHost);

        if (!proxyConfig.isAuthenticationEnabled()) {
            return builder;
        }

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(proxyConfig.getProxyUser(),
                proxyConfig.getProxyPassword().toCharArray());
        AuthScope authScope = new AuthScope(proxyHost);
        credentialsProvider.setCredentials(authScope, credentials);

        return builder.setDefaultCredentialsProvider(credentialsProvider);
    }
}
