// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.auth.response;

import com.boomi.util.Args;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.io.InputStream;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS;

/**
 * Utility class to parse Veeva API authentication responses.
 */
public class SessionResponseParser {

    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder().disable(FAIL_ON_UNKNOWN_PROPERTIES).disable(
            CAN_OVERRIDE_ACCESS_MODIFIERS).build();

    private SessionResponseParser() {
    }

    /**
     * Validates a Veeva Rest API auth response and parses its input stream verifying if it has the right format. If
     * that is the case, returns an {@link SessionResponse}. Otherwise, it throws an exception.
     *
     * @param response a http auth response
     * @return an {@link SessionResponse} instance
     * @throws IOException if the response is a failure or its input stream cannot be deserialized
     */
    public static SessionResponse parse(CloseableHttpResponse response) throws IOException {
        InputStream content = response.getEntity().getContent();
        try {
            SessionResponse sessionResponse = OBJECT_MAPPER.readValue(content, SessionResponse.class);

            Args.notNull(sessionResponse, "SessionResponse is not present.");
            Args.notNull(sessionResponse.getResponseStatus(), "Response Status is not present.");

            return sessionResponse;
        } catch (IllegalArgumentException e) {
            throw new IOException("The service response cannot be parsed. " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(content, response);
        }
    }
}
