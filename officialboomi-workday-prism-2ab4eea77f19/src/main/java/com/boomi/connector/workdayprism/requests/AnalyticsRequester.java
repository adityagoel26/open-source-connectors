//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.requests;

import com.boomi.connector.workdayprism.model.AuthProvider;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;

/**
 * An implementation of {@link Requester} aimed to make request to the different endpoints of the
 * prismAnalytics API.
 * It will set the url path and ensure that a Bearer Authorization token is included into the request.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class AnalyticsRequester extends AuthorizedRequester {
    private static final String BASE_PATH = "ccx/api/prismAnalytics/v2";

    private AnalyticsRequester(String httpMethod, int maxRetries, AuthProvider authProvider) {
        super(httpMethod, authProvider.getBasePath(BASE_PATH), maxRetries, authProvider);
    }

    /**
     * Static factory method to create and return a new {@link AnalyticsRequester} instance
     * specifically designeds for making GET requests to the prismAnalytics endpoints.
     *
     * @param authProvider
     *         a {@link AuthProvider} instance
     * @param maxRetries
     *         an int value indicating how many retries are allowed before a failed request is returned.
     */
    public static AnalyticsRequester get(AuthProvider authProvider, int maxRetries) {
        return new AnalyticsRequester(HttpGet.METHOD_NAME, maxRetries, authProvider);
    }

    /**
     * Static factory method to create and return a new {@link AnalyticsRequester} instance
     * specifically designeds for making POST requests to the prismAnalytics endpoints.
     *
     * @param authProvider
     *         a {@link AuthProvider} instance
     * @param maxRetries
     *         an int value indicating how many retries are allowed before a failed request is returned.
     */
    public static AnalyticsRequester post(AuthProvider authProvider, int maxRetries) {
        return new AnalyticsRequester(HttpPost.METHOD_NAME, maxRetries, authProvider);
    }


}
