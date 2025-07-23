// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.util;

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;

import java.util.function.Supplier;

/**
 * Utility class containing common logging functionality.
 */
public final class LogUtils {

    private static final String URL_METHOD_TEMPLATE = "%s : %s";

    private LogUtils() {
    }

    /**
     * Returns a Supplier that returns a String representation of the request method and URI
     *
     * @param requestBuilder of the request to log
     * @return Supplier that returns a String representation of the request method and URI
     */
    public static Supplier<String> requestMethodAndUriMessage(RequestBuilder requestBuilder) {
        return () -> String.format(URL_METHOD_TEMPLATE, requestBuilder.getMethod(), requestBuilder.getUri());
    }

    /**
     * Returns a Supplier that returns a String representation of the request method and URI
     *
     * @param request the request to log
     * @return Supplier that returns a String representation of the request method and URI
     */
    public static Supplier<String> requestMethodAndUriMessage(HttpUriRequest request) {
        return () -> String.format(URL_METHOD_TEMPLATE, request.getMethod(), request.getURI());
    }
}
