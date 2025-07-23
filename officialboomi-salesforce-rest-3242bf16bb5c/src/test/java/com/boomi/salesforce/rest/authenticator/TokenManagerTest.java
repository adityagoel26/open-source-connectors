// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.authenticator;

import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.request.AuthenticationRequestExecutor;
import com.boomi.salesforce.rest.request.RequestBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class TokenManagerTest {

    private static final Logger LOGGER_MOCK = Mockito.mock(Logger.class, Mockito.RETURNS_DEEP_STUBS);
    private static final RequestBuilder REQUEST_BUILDER_MOCK = Mockito.mock(RequestBuilder.class,
            Mockito.RETURNS_DEEP_STUBS);
    private static final AuthenticationRequestExecutor REQUEST_EXECUTOR_MOCK = Mockito.mock(
            AuthenticationRequestExecutor.class, Mockito.RETURNS_DEEP_STUBS);

    static Stream<Arguments> getTokenManagerArguments() {
        // SOAP authentication
        MutablePropertyMap soapProperties = new MutablePropertyMap();
        soapProperties.put("authenticationType", AuthenticationType.USER_CREDENTIALS.name());
        soapProperties.put("url", "https://example.com/salesforce");
        soapProperties.put("username", "theusername");
        soapProperties.put("authenticationUrl", "https://login.salesforce.com/services/Soap/u/48.0");

        ConcurrentHashMap<Object, Object> soapCache = new ConcurrentHashMap<>();
        /* We need to pre-populate this cache entry, so when the TokenManager is instantiated then an HTTP request
        won't be executed.*/
        soapCache.put("credentials.theusername.https://login.salesforce.com/services/Soap/u/48.0", "sessionID");

        // OAuth authentication
        MutablePropertyMap oauthProperties = new MutablePropertyMap();
        oauthProperties.put("authenticationType", AuthenticationType.OAUTH_2.name());

        // Test cases
        return Stream.of(Arguments.of(new ConnectionProperties(soapProperties, soapCache, LOGGER_MOCK),
                        SFSoapAuthenticator.class),
                Arguments.of(new ConnectionProperties(oauthProperties, new ConcurrentHashMap<>(), LOGGER_MOCK),
                        SFOAuth2Authenticator.class));
    }

    @ParameterizedTest
    @MethodSource("getTokenManagerArguments")
    void getTokenManager(ConnectionProperties connection, Class<?> expectedType) {
        TokenManager tokenManager = TokenManager.getTokenManager(connection, REQUEST_BUILDER_MOCK,
                REQUEST_EXECUTOR_MOCK);

        Assertions.assertInstanceOf(expectedType, tokenManager);
    }

    @Test
    void getTokenManagerWithInvalidAuthenticationType() {
        MutablePropertyMap properties = new MutablePropertyMap();
        properties.put("authenticationType", "invalid");

        ConnectionProperties connection = new ConnectionProperties(properties, new ConcurrentHashMap<>(), LOGGER_MOCK);

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TokenManager.getTokenManager(connection, REQUEST_BUILDER_MOCK, REQUEST_EXECUTOR_MOCK));
    }
}
