// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.data.JSONValueSafeExtractor;
import com.boomi.util.IOUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;

import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Designed to parse small sized Metadata responses.<br> Closes each stream after parsing.
 */
public class JSONUtils {

    /**
     * Jackson 2.15.x introduces limits to the string fields length that can be parsed. This JsonFactory overrides
     * such validations to avoid introducing a regression in existing processes handling potentially large values
     */
    private static final JsonFactory JSON_FACTORY = JsonFactory.builder().streamReadConstraints(
            StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build()).build();

    private JSONUtils() {
    }

    /**
     * Parses the Response InputStream content to get the value of a key, returns null if the key was not found
     *
     * @return string content of the target key
     * @throws ConnectorException when failed to parse JSON content
     */
    public static Map<String, String> getValuesMapSafely(ClassicHttpResponse response, List<String> targetValues) {
        JSONValueSafeExtractor extractor = null;
        try {
            extractor = new JSONValueSafeExtractor(response, targetValues);
            return extractor.getValuesMap();
        } catch (IOException e) {
            throw new ConnectorException("[Errors occurred while parsing JSON] " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(extractor);
        }
    }

    /**
     * Parses the Response InputStream content to get the value of a key, returns null if the key was not found
     *
     * @return string content of the target key
     * @throws ConnectorException when failed to parse JSON content
     */
    public static String getValueSafely(ClassicHttpResponse response, String key) {
        return getValuesMapSafely(response, Collections.singletonList(key)).getOrDefault(key, "");
    }

    /**
     * Get a JsonFactory configured to circumvent field size validations
     *
     * @return the JsonFactory
     */
    public static JsonFactory getJsonFactory() {
        return JSON_FACTORY;
    }
}
