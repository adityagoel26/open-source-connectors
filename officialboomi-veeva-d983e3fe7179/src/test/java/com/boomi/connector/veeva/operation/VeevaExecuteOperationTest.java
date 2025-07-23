// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.veeva.operation;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleOperationContext;
import com.boomi.connector.veeva.VeevaOperationConnection;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StreamUtil;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VeevaExecuteOperationTest {

    public static Stream<Arguments> clientIDSource() {
        return Stream.of(arguments("header_value", "Boomi_header_value"), arguments("", "Boomi"));
    }

    @MethodSource("clientIDSource")
    @ParameterizedTest()
    void VeevaExecuteOperationGetHeaders(String clientID, String expectedClientID) {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        when(connection.getContext().getConnectionProperties().getProperty("veevaClientID")).thenReturn(clientID);
        when(connection.getContext().getObjectTypeId()).thenReturn("theObjectType");
        ObjectData data = mock(ObjectData.class);

        Collection<Map.Entry<String, String>> headersIterable = Arrays.asList(
                CollectionUtil.newImmutableEntry("Content-Type", "application/json"),
                CollectionUtil.newImmutableEntry("X-VaultAPI-ClientID", expectedClientID));

        VeevaExecuteOperation executeOperation = new VeevaExecuteOperation(connection);

        assertIterableEquals(headersIterable, executeOperation.getHeaders(data));
    }

    @MethodSource("clientIDSource")
    @ParameterizedTest()
    void getHeadersWithoutContentTypeTest(String clientID, String expectedClientID) {
        VeevaOperationConnection connectionMock = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        OperationContext contextMock = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class);

        when(contextMock.getCustomOperationType()).thenReturn("POST");
        when(contextMock.getConnectionProperties().getProperty("veevaClientID")).thenReturn(clientID);
        when(connectionMock.getContext()).thenReturn(contextMock);
        when(connectionMock.getContext().getObjectTypeId()).thenReturn("POST::/services/file_staging/items");
        when(connectionMock.isOpenApiOperation()).thenReturn(true);

        Collection<Map.Entry<String, String>> headers = Collections.singleton(
                CollectionUtil.newImmutableEntry("X-VaultAPI-ClientID", expectedClientID));

        assertEquals(headers, new VeevaExecuteOperation(connectionMock).getHeaders(dataMock));
    }

    @Test
    void getPathTest() {
        OperationContext contextMock = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class);

        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("overrideApiVersion", true);

        PropertyMap connectionProperties = new MutablePropertyMap();
        connectionProperties.put("authenticationType", "USER_CREDENTIALS");
        connectionProperties.put("vaultSubdomain", "subdomain.veeva.com");
        connectionProperties.put("apiVersion", "v23.3");
        connectionProperties.put("username", "the username");
        connectionProperties.put("password", "the password");
        connectionProperties.put("sessionTimeout", 10L);
        when(contextMock.getConnectionProperties()).thenReturn(connectionProperties);
        when(contextMock.getOperationProperties()).thenReturn(operationProperties);
        when(contextMock.getObjectTypeId()).thenReturn("product");

        MutableDynamicPropertyMap dynamicOperationProperties = new MutableDynamicPropertyMap();
        dynamicOperationProperties.addProperty("operationApiVersion", "v22.1");
        when(dataMock.getDynamicOperationProperties()).thenReturn(dynamicOperationProperties);

        VeevaOperationConnection connection = new VeevaOperationConnection(contextMock);

        assertEquals("/vobjects/product", new VeevaExecuteOperation(connection).getPath(dataMock));
    }

    @Test
    void getParametersMultipartTest() {
        VeevaOperationConnection connectionMock = mock(VeevaOperationConnection.class);
        OperationContext contextMock = mock(OperationContext.class);
        ObjectData dataMock = mock(ObjectData.class);

        when(contextMock.getCustomOperationType()).thenReturn("OPEN_API");
        when(contextMock.getObjectTypeId()).thenReturn("POST::/services/file_staging/items");
        when(connectionMock.getContext()).thenReturn(contextMock);
        when(connectionMock.isOpenApiOperation()).thenReturn(true);

        assertEquals(Collections.emptyList(), new VeevaExecuteOperation(connectionMock).getParameters(dataMock));
    }

    @Test
    void getParametersOpenApiTest() {
        VeevaOperationConnection connectionMock = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        OperationContext contextMock = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class);

        when(contextMock.getCustomOperationType()).thenReturn("OPEN_API");
        when(contextMock.getObjectTypeId()).thenReturn("POST::/objects/documents/batch");
        when(contextMock.getConnectionProperties().getProperty("auth")).thenReturn("API_KEY");
        when(connectionMock.getContext()).thenReturn(contextMock);
        when(connectionMock.isOpenApiOperation()).thenReturn(true);

        new VeevaExecuteOperation(connectionMock).getParameters(dataMock);

        Mockito.verify(connectionMock, atLeastOnce()).getRequestCookie();
    }

    @Test
    void getParametersCustomTest() {
        VeevaOperationConnection connectionMock = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        OperationContext contextMock = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class);

        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("QUERYPARAM_myQueryParam1", "queryParamValue");
        operationProperties.put("PATHPARAM__myPathParam1", "pathParamValue");

        when(contextMock.getCustomOperationType()).thenReturn("GET");
        when(contextMock.getObjectTypeId()).thenReturn("Product");
        when(contextMock.getOperationProperties()).thenReturn(operationProperties);
        when(connectionMock.getContext()).thenReturn(contextMock);

        Iterable<Map.Entry<String, String>> params = new VeevaExecuteOperation(connectionMock).getParameters(dataMock);

        assertEquals(1, StreamSupport.stream(params.spliterator(), false).count());
        assertEquals("queryParamValue", params.iterator().next().getValue());
    }

    @Test
    void getInputStreamEntityTest() throws IOException {
        VeevaOperationConnection connectionMock = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        SimpleOperationContext contextMock = mock(SimpleOperationContext.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class);

        when(contextMock.getCustomOperationType()).thenReturn("OPEN_API");
        when(contextMock.getObjectTypeId()).thenReturn("POST::/objects/documents/batch");
        when(contextMock.createTempOutputStream()).thenCallRealMethod();
        when(contextMock.tempOutputStreamToInputStream(any(OutputStream.class))).thenCallRealMethod();
        when(connectionMock.getContext()).thenReturn(contextMock);
        when(connectionMock.isOpenApiOperation()).thenReturn(true);
        when(dataMock.getData()).thenReturn(
                new ByteArrayInputStream("[{\"a\":\"a\"}]".getBytes(StandardCharsets.UTF_8)));

        HttpEntity entity = new VeevaExecuteOperation(connectionMock).getEntity(dataMock);

        assertTrue(entity.isRepeatable());
        assertEquals("\"a\"\n\"a\"\n", StreamUtil.toString(entity.getContent(), StandardCharsets.UTF_8));
    }

    @Test
    void getMultipartEntityTest() throws IOException {
        VeevaOperationConnection connectionMock = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        SimpleOperationContext contextMock = mock(SimpleOperationContext.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class);

        when(contextMock.getCustomOperationType()).thenReturn("OPEN_API");
        when(contextMock.getObjectTypeId()).thenReturn("POST::/services/file_staging/items");
        when(connectionMock.getContext()).thenReturn(contextMock);
        when(dataMock.getData()).thenReturn(new ByteArrayInputStream("aaa".getBytes(StandardCharsets.UTF_8)));
        when(connectionMock.isOpenApiOperation()).thenReturn(true);

        HttpEntity entity = new VeevaExecuteOperation(connectionMock).getEntity(dataMock);

        assertTrue((entity.getContentType()).getValue().contains(ContentType.MULTIPART_FORM_DATA.getMimeType()));
    }

    @Test
    void customHeadersTest() {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class, RETURNS_DEEP_STUBS);

        // Create a map with both empty and non-empty headers
        Map<String, String> testHeaders = new HashMap<>();
        testHeaders.put("validHeader", "value");
        testHeaders.put("emptyHeader", "");
        testHeaders.put("nullHeader", null);

        when(connection.getCustomHeaders(dataMock)).thenReturn(testHeaders);

        VeevaExecuteOperation executeOperation = new VeevaExecuteOperation(connection);
        Collection<Map.Entry<String, String>> headers =
                (Collection<Map.Entry<String, String>>) executeOperation.getHeaders(dataMock);

        //Check that the Content-Type, X-VaultAPI-ClientID, and validHeader are present.
        assertEquals(3, headers.size());

        for (Map.Entry<String, String> header : headers) {
            switch (header.getKey()) {
                case "Content-Type":
                    assertEquals("application/json", header.getValue());
                    break;
                case "X-VaultAPI-ClientID":
                    assertEquals("Boomi", header.getValue());
                    break;
                case "validHeader":
                    assertEquals("value", header.getValue());
                    break;
                default:
                    fail("Unexpected header in HTTP request: " + header);
            }
        }
    }

    @Test
    void customHeadersReturnsEmptyMap() {
        VeevaOperationConnection connection = mock(VeevaOperationConnection.class, RETURNS_DEEP_STUBS);
        ObjectData dataMock = mock(ObjectData.class, RETURNS_DEEP_STUBS);

        when(connection.getCustomHeaders(dataMock)).thenReturn(Collections.emptyMap());

        VeevaExecuteOperation executeOperation = new VeevaExecuteOperation(connection);
        Collection<Map.Entry<String, String>> headers =
                (Collection<Map.Entry<String, String>>) executeOperation.getHeaders(dataMock);

        //Check that the Content-Type and X-VaultAPI-ClientID are present.
        assertEquals(2, headers.size());

        for (Map.Entry<String, String> header : headers) {
            switch (header.getKey()) {
                case "Content-Type":
                    assertEquals("application/json", header.getValue());
                    break;
                case "X-VaultAPI-ClientID":
                    assertEquals("Boomi", header.getValue());
                    break;
                default:
                    fail("Unexpected header in HTTP request: " + header);
            }
        }
    }
}
