// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.salesforce.rest.testutil.SFRestContextIT;
import com.boomi.salesforce.rest.testutil.SFRestTestUtil;
import com.boomi.util.CollectionUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SFRestBrowserTest {

    private static final String MESSAGE = "Message";
    private SFRestConnection _connectionMock;

    @BeforeEach
    void setup() {
        _connectionMock = mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);
    }

    @Test
    void shouldCallGetConnection() {
        assertNotNull(new SFRestBrowser(_connectionMock).getConnection());
    }

    @Test
    void shouldCloseConnectionWhenInitializeThrowConnectorException() {
        Mockito.doThrow(new ConnectorException(MESSAGE)).when(_connectionMock).initialize();
        try {
            Assertions.assertThrows(ConnectorException.class,
                    () -> new SFRestBrowser(_connectionMock).getObjectTypes());
        } finally {
            verify(_connectionMock).close();
        }
    }

    public static Stream<Arguments> getObjectDefinitionsTestCases() {
        return Stream.of(arguments(OperationType.CREATE.name()), //
                arguments(OperationType.DELETE.name()), //
                arguments(OperationType.UPDATE.name()), //
                arguments(OperationType.UPSERT.name()), //
                arguments(OperationType.QUERY.name()));
    }

    @ParameterizedTest
    @MethodSource("getObjectDefinitionsTestCases")
    void getObjectDefinitionsForPriceBookEntryTest(String operationType) throws IOException {
        ClassicHttpResponse responseMock = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);

        when(_connectionMock.getOperationProperties().getOperationBoomiName()).thenReturn(operationType);
        when(_connectionMock.getContext()).thenReturn(new SFRestContextIT());
        when(responseMock.getEntity().getContent()).thenReturn(
                SFRestTestUtil.getContent("pricebookentryDescribeResponse.json"));
        when(_connectionMock.getRequestHandler().executeGetFields("PriceBookEntry")).thenReturn(responseMock);

        SFRestBrowser browser = new SFRestBrowser(_connectionMock);

        ObjectDefinitions result = browser.getObjectDefinitions("PriceBookEntry",
                Arrays.asList(ObjectDefinitionRole.values()));

        assertEquals(1, result.getDefinitions().size());
        ObjectDefinition definition = CollectionUtil.getFirst(result.getDefinitions());
        assertEquals(ContentType.XML, definition.getInputType());
        assertEquals(ContentType.XML, definition.getOutputType());

        verify(_connectionMock, atLeast(1)).close();
    }

    @Test
    void getObjectDefinitionsForCreateTreeOperationTest() throws IOException {
        ClassicHttpResponse responseMock = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);
        SFRestContextIT context = new SFRestContextIT();
        context.setOperationCustomType("CREATE_TREE");

        when(_connectionMock.getOperationProperties().getOperationBoomiName()).thenReturn(OperationType.CREATE.name());
        when(_connectionMock.getContext()).thenReturn(context);
        when(responseMock.getEntity().getContent()).thenReturn(
                SFRestTestUtil.getContent("pricebookentryDescribeResponse.json"));
        when(_connectionMock.getRequestHandler().executeGetFields("PriceBookEntry")).thenReturn(responseMock);

        SFRestBrowser browser = new SFRestBrowser(_connectionMock);

        ObjectDefinitions result = browser.getObjectDefinitions("PriceBookEntry",
                Arrays.asList(ObjectDefinitionRole.values()));

        assertEquals(1, result.getDefinitions().size());
        ObjectDefinition definition = CollectionUtil.getFirst(result.getDefinitions());
        assertEquals(ContentType.XML, definition.getInputType());
        assertEquals(ContentType.XML, definition.getOutputType());

        verify(_connectionMock, atLeast(1)).close();
    }

    public static Stream<Arguments> getObjectTypesTestCases() {
        return Stream.of(arguments(OperationType.QUERY, 931), //
                arguments(OperationType.DELETE, 563), //
                arguments(OperationType.CREATE, 459), //
                arguments(OperationType.UPDATE, 454), //
                arguments(OperationType.UPSERT, 454));
    }

    @Test
    void getObjectDefinitionsWithConnectionFailure() {
        SFRestContextIT context = new SFRestContextIT();

        when(_connectionMock.getContext()).thenReturn(context);
        doThrow(new ConnectorException("error")).when(_connectionMock).initialize();

        SFRestBrowser browser = new SFRestBrowser(_connectionMock);

        assertThrows(ConnectorException.class, browser::getObjectTypes);

        verify(_connectionMock, atLeast(1)).close();
    }

    @ParameterizedTest
    @MethodSource("getObjectTypesTestCases")
    void getObjectTypes(OperationType operationType, int expectedObjectTypes) throws IOException {
        SFRestContextIT context = new SFRestContextIT();
        context.setOperationType(operationType);
        ClassicHttpResponse responseMock = mock(ClassicHttpResponse.class, RETURNS_DEEP_STUBS);

        when(_connectionMock.getContext()).thenReturn(context);
        when(_connectionMock.getRequestHandler().executeGetObjects()).thenReturn(responseMock);
        when(responseMock.getEntity().getContent()).thenReturn(
                SFRestTestUtil.getContent("getObjectTypesResponse.json"));

        SFRestBrowser browser = new SFRestBrowser(_connectionMock);
        ObjectTypes objectTypes = browser.getObjectTypes();

        assertEquals(expectedObjectTypes, objectTypes.getTypes().size());

        verify(_connectionMock, atLeast(1)).close();
    }

    @Test
    void getObjectTypesWithUnsupportedOperationType() {
        SFRestContextIT context = new SFRestContextIT();
        context.setOperationType(OperationType.GET);

        when(_connectionMock.getContext()).thenReturn(context);

        SFRestBrowser browser = new SFRestBrowser(_connectionMock);
        assertThrows(UnsupportedOperationException.class, browser::getObjectTypes);
        verify(_connectionMock, atLeast(1)).close();
    }

    @Test
    void testConnection() {
        SFRestBrowser browser = new SFRestBrowser(_connectionMock);
        browser.testConnection();

        verify(_connectionMock, times(1)).initialize();
        verify(_connectionMock, atLeast(1)).close();
    }

    @Test
    void testConnectionMustPropagateException() {
        doThrow(new ConnectorException("expected!")).when(_connectionMock).initialize();

        SFRestBrowser browser = new SFRestBrowser(_connectionMock);

        assertThrows(ConnectorException.class, browser::testConnection);

        verify(_connectionMock, times(1)).initialize();
        verify(_connectionMock, atLeast(1)).close();
    }
}
