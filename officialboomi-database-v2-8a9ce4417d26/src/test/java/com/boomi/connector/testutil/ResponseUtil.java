// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.testutil;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Response Utility class
 */
public final class ResponseUtil {

    /** The Constant EXECUTION_ID. */
    public static final String EXECUTION_ID = "executionId";

    private ResponseUtil() {
        //Preventing object creation
    }

    /**
     * This method will return the SimpleOperationResponse object using passed argument
     *
     * @param documents documents passed
     * @return SimpleOperationResponse object
     */
    public static SimpleOperationResponse getResponse(Iterable<? extends SimpleTrackedData> documents) {
        SimpleOperationResponse response = new SimpleOperationResponse();
        for (SimpleTrackedData document : documents) {
            response.addTrackedData(document);
        }

        return response;
    }

    /** Normalizes a JSON string by parsing and re-serializing it to ensure consistent formatting.
     * @param json The JSON string to normalize
     * @return The normalized JSON string
     * @throws JsonProcessingException If an error occurs during JSON processing
     */
    public static String normalizeJson(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(objectMapper.readTree(json));
    }

    /**
     * Creates SimpleTrackedData from String
     * @param payload
     * @return
     */
    public static SimpleTrackedData createInputDocument(String payload) {
        InputStream messagePayload = new ByteArrayInputStream(payload.getBytes(StringUtil.UTF8_CHARSET));
        return new SimpleTrackedData(13, messagePayload);
    }

    /**
     * Get {@link UpdateRequest}
     * @param documents
     * @return
     */
    public static UpdateRequest toRequest(final Iterable<ObjectData> documents) {
        return new UpdateRequest() {

            /**
             * Returns an iterator over elements of type {@code T}.
             *
             * @return an Iterator.
             */
            @Override
            public Iterator<ObjectData> iterator() {
                return documents.iterator();
            }

            /**
             * Get the id for the current process component
             *
             * @return the process id
             */
            @Override
            public String getProcessId() {
                return null;
            }

            /**
             * @return
             */
            @Override
            public String getTopLevelProcessId() {
                return null;
            }

            /**
             * Get the id for the current process execution
             *
             * @return the execution id
             */
            @Override
            public String getExecutionId() {
                return null;
            }

            /**
             * @return
             */
            @Override
            public String getTopLevelExecutionId() {
                return EXECUTION_ID;
            }
        };
    }
}