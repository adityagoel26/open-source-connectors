// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import com.boomi.common.apache.http.response.HttpResponseUtil;
import com.boomi.util.StreamUtil;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;

import java.io.IOException;
import java.io.InputStream;

public final class HubspotcrmResponseUtil {

    private static final String X_API_STATUS = "X-API-Status";
    private static final String SUCCESS = "SUCCESS";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_JSON = "application/json";

    private HubspotcrmResponseUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Utility method that accepts a response and, provided it does not include a header {@link #X_API_STATUS}
     * with value {@link #SUCCESS} and it has content of type json, copy the original content stream into a temp stream,
     * close the original and replace it with the copy.
     * <p>
     * This is useful when the response content stream needs to be consumed more than once, considering the temp stream
     * can be reset.
     *
     * @param response an HTTP response
     * @return the HTTP response with a temp stream that can be reset
     * @throws IOException if an I/O error occurs with the input stream
     */
    public static CloseableHttpResponse getWithResettableContentStream(CloseableHttpResponse response)
            throws IOException {
        if ((!isApiStatusSuccess(response) || isContentTypeJson(response) ) && HttpResponseUtil.isEntityAvailable(
                response)) {
            InputStream streamCopy;
            try (InputStream originalContent = response.getEntity().getContent()) {
                streamCopy = StreamUtil.tempCopy(originalContent);
            }
            response.setEntity(new InputStreamEntity(streamCopy));
        }
        return response;
    }

    /**
     * Checks if the Vault API response indicates a successful operation.
     *
     * @param response The CloseableHttpResponse object representing the Vault API response.
     * @return true if the response indicates a successful operation, false otherwise.
     */
    private static boolean isApiStatusSuccess(CloseableHttpResponse response) {
        Header contentTypeHeader = response.getFirstHeader(X_API_STATUS);
        return contentTypeHeader != null && contentTypeHeader.getValue().equals(SUCCESS);
    }

    /**
     * Checks if the response from the server has a Content-Type header indicating JSON.
     *
     * @param response the {@link CloseableHttpResponse} object representing the server response
     * @return true if the Content-Type header indicates JSON, false otherwise
     */
   private static boolean isContentTypeJson(CloseableHttpResponse response) {
        Header contentTypeHeader = response.getFirstHeader(CONTENT_TYPE);
        return contentTypeHeader != null && contentTypeHeader.getValue().contains(APPLICATION_JSON);
    }
}
