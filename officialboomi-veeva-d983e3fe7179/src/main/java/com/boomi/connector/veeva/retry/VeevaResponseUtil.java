// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.retry;

import com.boomi.common.apache.http.response.HttpResponseUtil;
import com.boomi.util.StreamUtil;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;

import java.io.IOException;
import java.io.InputStream;

public final class VeevaResponseUtil {

    private static final String X_VAULT_API_STATUS = "X-VaultAPI-Status";
    private static final String SUCCESS = "SUCCESS";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String APPLICATION_JSON = "application/json";

    private VeevaResponseUtil() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Utility method that accepts a response and, provided it does not include a header {@link #X_VAULT_API_STATUS}
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

        if (isVaultApiStatusSuccess(response) || !isContentTypeJson(response) || !HttpResponseUtil.isEntityAvailable(
                response)) {
            // the response includes a header stating it was successful or the response does not have json content
            return response;
        }

        InputStream streamCopy;
        try (InputStream originalContent = response.getEntity().getContent()) {
            streamCopy = StreamUtil.tempCopy(originalContent);
        }

        response.setEntity(new InputStreamEntity(streamCopy));
        return response;
    }

    static boolean isVaultApiStatusSuccess(CloseableHttpResponse response) {
        Header contentTypeHeader = response.getFirstHeader(X_VAULT_API_STATUS);
        return contentTypeHeader != null && contentTypeHeader.getValue().equals(SUCCESS);
    }

    static boolean isContentTypeJson(CloseableHttpResponse response) {
        Header contentTypeHeader = response.getFirstHeader(CONTENT_TYPE);
        return contentTypeHeader != null && contentTypeHeader.getValue().contains(APPLICATION_JSON);
    }

    static boolean hasContent(CloseableHttpResponse response) {
        return HttpResponseUtil.isEntityAvailable(response);
    }
}
