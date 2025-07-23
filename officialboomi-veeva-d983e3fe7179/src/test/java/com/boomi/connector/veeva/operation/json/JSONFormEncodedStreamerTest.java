// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.veeva.operation.json;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.TempOutputStream;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JSONFormEncodedStreamerTest {

    private static final int TWO_LOOPS = 2;

    private JSONFormEncodedStreamer _underTest;
    @Mock
    private InputStream _inputStreamMock;
    @Mock
    private JsonFactory _jsonFactoryMock;
    @Mock
    private JsonParser _jsonParserMock;

    @BeforeEach
    public void setUp() {
        _inputStreamMock = mock(InputStream.class);
        _jsonFactoryMock = mock(JsonFactory.class);
        _jsonParserMock = mock(JsonParser.class);
    }

    @ParameterizedTest
    @CsvSource({
            "firstName, firstNameASString,,, firstName=firstNameASString, 1",
            "firstName, firstNameASString, secondName, secondValueAsString, "
            + "firstName=firstNameASString&secondName=secondValueAsString, 2",
            "firstName, firstNameASString,,, '', 0", })
    public void jsonToFormEncodingTestSuccess(String currentName, String valueAsString, String secondName,
            String secondValueAsString, String outputExpected, String numberOfLoops) throws IOException {
        //Arrange
        String outputResult;
        TempOutputStream outputStream = new TempOutputStream();
        //JsonFactory
        when(_jsonFactoryMock.createParser(any(InputStream.class))).thenReturn(_jsonParserMock);

        //JsonParser
        if (Integer.parseInt(numberOfLoops) == 1) {
            when(_jsonParserMock.nextToken()).thenReturn(JsonToken.START_OBJECT, JsonToken.FIELD_NAME,
                    JsonToken.VALUE_STRING);
            doReturn(JsonToken.FIELD_NAME).doReturn(JsonToken.VALUE_STRING).doReturn(JsonToken.END_OBJECT).when(
                    _jsonParserMock).getCurrentToken();

            when(_jsonParserMock.getCurrentName()).thenReturn(currentName);
            when(_jsonParserMock.getValueAsString()).thenReturn(valueAsString);
        } else if (Integer.parseInt(numberOfLoops) == TWO_LOOPS) {
            when(_jsonParserMock.nextToken()).thenReturn(JsonToken.START_OBJECT, JsonToken.FIELD_NAME,
                    JsonToken.VALUE_STRING, JsonToken.FIELD_NAME, JsonToken.VALUE_STRING);
            doReturn(JsonToken.FIELD_NAME).doReturn(JsonToken.VALUE_STRING).doReturn(JsonToken.FIELD_NAME).doReturn(
                    JsonToken.FIELD_NAME).doReturn(JsonToken.VALUE_STRING).doReturn(JsonToken.END_OBJECT).when(
                    _jsonParserMock).getCurrentToken();

            doReturn(currentName).doReturn(secondName).when(_jsonParserMock).getCurrentName();
            doReturn(valueAsString).doReturn(secondValueAsString).when(_jsonParserMock).getValueAsString();
        } else {
            when(_jsonParserMock.nextToken()).thenReturn(null);
        }

        _underTest = new JSONFormEncodedStreamer(_jsonFactoryMock);

        //Act
        _underTest.fromJsonToStream(_inputStreamMock, outputStream);
        //Assert
        try (BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(outputStream.toInputStream(), StandardCharsets.UTF_8))) {
            outputResult = bufferedReader.lines().collect(Collectors.joining());
        }

        assertNotNull(outputResult);
        assertEquals(outputExpected, outputResult);
    }

    @ParameterizedTest
    @CsvSource({
            "validateJsonTokenObject, JSON start object expected but got: END_OBJECT",
            "validateJsonValue, JSON value expected but got FIELD_NAME" })
    public void jsonToFormEncodingTestException(String validationType, String errorExpected) throws IOException {
        //Arrange
        TempOutputStream outputStream = new TempOutputStream();
        //JsonFactory
        when(_jsonFactoryMock.createParser(any(InputStream.class))).thenReturn(_jsonParserMock);

        //JsonParser
        if (validationType.equals("validateJsonTokenObject")) {
            when(_jsonParserMock.nextToken()).thenReturn(JsonToken.END_OBJECT);
        } else {
            when(_jsonParserMock.nextToken()).thenReturn(JsonToken.START_OBJECT);
            when(_jsonParserMock.getCurrentToken()).thenReturn(JsonToken.FIELD_NAME).thenReturn(JsonToken.FIELD_NAME);
        }

        //Act
        _underTest = new JSONFormEncodedStreamer(_jsonFactoryMock);
        ConnectorException exception = assertThrows(ConnectorException.class,
                () -> _underTest.fromJsonToStream(_inputStreamMock, outputStream));
        assertTrue(exception.getMessage().contains(errorExpected));
    }

    @Test
    public void jsonToFormEncodingExceptionTest() throws IOException {
        // expected exception
        String expectedMessage =
                "Unknown failure: java.io.IOException: InputStream.read() returned 0 characters when trying to read "
                + "8000 bytes";

        try (InputStream is = mock(InputStream.class); OutputStream os = mock(OutputStream.class)) {
            ConnectorException exception = assertThrows(ConnectorException.class,
                    () -> new JSONFormEncodedStreamer(JSONUtil.getDefaultJsonFactory()).fromJsonToStream(is, os));
            assertTrue(exception.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void jsonToFormEncodingStartObjectExceptionTest() throws IOException {
        JsonFactory factory = mock(JsonFactory.class);
        JsonParser parser = mock(JsonParser.class);
        when(factory.createParser(any(InputStream.class))).thenReturn(parser);
        when(parser.nextToken()).thenReturn(JsonToken.START_ARRAY);

        // expected exception
        String expectedMessage = "JSON start object expected but got: " + JsonToken.START_ARRAY.name();

        try (InputStream is = mock(InputStream.class); OutputStream os = mock(OutputStream.class)) {
            ConnectorException exception = assertThrows(ConnectorException.class,
                    () -> new JSONFormEncodedStreamer(factory).fromJsonToStream(is, os));
            assertTrue(exception.getMessage().contains(expectedMessage));
        }
    }

    @Test
    public void jsonToFormEncodingValueExpectedExceptionTest() throws IOException {
        JsonFactory factory = mock(JsonFactory.class);
        JsonParser parser = mock(JsonParser.class);
        when(factory.createParser(any(InputStream.class))).thenReturn(parser);
        when(parser.nextToken()).thenReturn(JsonToken.START_OBJECT, JsonToken.FIELD_NAME, JsonToken.START_ARRAY);
        doReturn(JsonToken.FIELD_NAME).doReturn(JsonToken.START_ARRAY).when(parser).getCurrentToken();

        // expected exception
        String expectedMessage = "JSON value expected but got " + JsonToken.START_ARRAY.name();

        try (InputStream is = mock(InputStream.class); OutputStream os = mock(OutputStream.class)) {
            ConnectorException exception = assertThrows(ConnectorException.class,
                    () -> new JSONFormEncodedStreamer(factory).fromJsonToStream(is, os));
            assertTrue(exception.getMessage().contains(expectedMessage));
        }
    }
}
