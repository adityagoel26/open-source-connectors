// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Utility class to build {@link HttpClient}s configured with proxy settings, provided those are available.
 */

public class HttpClientFactory {

    private HttpClientFactory() {
        throw new IllegalStateException("Utility class");
    }

    /**
 * Creates and returns a new instance of CloseableHttpClient with automatic retries disabled.
 *
 * @return A new instance of CloseableHttpClient.
 */
public static CloseableHttpClient createHttpClient() {
    HttpClientBuilder builder = HttpClientBuilder.create().disableAutomaticRetries();
    return builder.build();
}
}
