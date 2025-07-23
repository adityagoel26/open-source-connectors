// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.properties;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.util.StringUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ConnectionPropertiesTest {

    public static Stream<Arguments> validURLs() {
        // input | expected
        return Stream.of(arguments("https://example.com/", "https://example.com"),
                arguments("https://example.com/path/", "https://example.com/path"),
                arguments("https://example.com", "https://example.com"),
                arguments("https://example.com/path", "https://example.com/path"));
    }

    @ParameterizedTest
    @MethodSource("validURLs")
    void getURLWithValidInputs(String input, String expected) {
        ConcurrentMap<Object, Object> cache = new ConcurrentHashMap<>();
        Logger logger = null;
        MutablePropertyMap config = new MutablePropertyMap();
        config.put("url", input);

        ConnectionProperties connectionProperties = new ConnectionProperties(config, cache, logger);

        assertEquals(expected, connectionProperties.getURL().toString());
    }

    public static Stream<Arguments> invalidURLs() {
        return Stream.of(
                // url not provided
                arguments(StringUtil.EMPTY_STRING), arguments(new Object[] { null }),
                // missing protocol
                arguments("example.com"));
    }

    @ParameterizedTest
    @MethodSource("invalidURLs")
    void getURLWithInvalidInputs(String input) {
        ConcurrentMap<Object, Object> cache = new ConcurrentHashMap<>();
        Logger logger = null;
        MutablePropertyMap config = new MutablePropertyMap();
        config.put("url", input);

        ConnectionProperties connectionProperties = new ConnectionProperties(config, cache, logger);

        assertThrows(ConnectorException.class, connectionProperties::getURL);
    }

    @Test
    void getAuthenticationUrlTest() {
        ConcurrentMap<Object, Object> cache = new ConcurrentHashMap<>();
        Logger logger = null;
        MutablePropertyMap config = new MutablePropertyMap();
        config.put("url", "https://example.com");
        //URL with intentional whitespaces in the beginning and in the end.
        config.put("authenticationUrl", "    https://example.com/auth   ");

        ConnectionProperties connectionProperties = new ConnectionProperties(config, cache, logger);
        //make sure that the URL is being trimmed.
        assertEquals("https://example.com/auth", connectionProperties.getAuthenticationUrl().toString());
    }

    @Test
    void blankAuthenticationUrlTest() {
        ConcurrentMap<Object, Object> cache = new ConcurrentHashMap<>();
        Logger logger = null;
        MutablePropertyMap config = new MutablePropertyMap();
        config.put("url", "https://example.com");
        //URL with intentional whitespaces in the beginning and in the end.
        config.put("authenticationUrl", "       ");

        ConnectionProperties connectionProperties = new ConnectionProperties(config, cache, logger);
        //make sure that the URL is being trimmed.
        assertEquals("https://login.salesforce.com/services/Soap/u/48.0", connectionProperties.getAuthenticationUrl().toString());
    }
}
