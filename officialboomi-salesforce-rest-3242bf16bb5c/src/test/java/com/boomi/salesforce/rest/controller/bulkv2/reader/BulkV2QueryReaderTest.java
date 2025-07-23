// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2.reader;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.salesforce.rest.request.RequestHandler;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkV2QueryReaderTest {

    private static final String BULK_RESPONSE_CHARS_LIMIT_EXCEEDED_ERROR =
            "[Failed to read bulk response header] Characters limits exceeded 1,000,000 characters: java.io"
                    + ".IOException: Characters limits exceeded 1,000,000 characters";
    private static final String CSV_CHAR_LIMIT_EXCEEDED_ERROR =
            "[Failed to read CSV] Characters limits exceeded 1,000,000 characters: java.io.IOException: "
                    + "Characters limits exceeded 1,000,000 characters";
    private final ObjectData _objectData = mock(ObjectData.class);
    private final UpdateRequest _updateRequest = mock(UpdateRequest.class);
    private final Logger _logger = mock(Logger.class);
    private final SFRestConnection _connection = mock(SFRestConnection.class, RETURNS_DEEP_STUBS);
    private final RequestHandler _handler = mock(RequestHandler.class, RETURNS_DEEP_STUBS);
    private final ConnectionProperties _connectionProperties = mock(ConnectionProperties.class);
    private final OperationProperties _operationProperties = mock(OperationProperties.class);
    private final ClassicHttpResponse _response = mock(ClassicHttpResponse.class);
    private final HttpEntity _entity = mock(HttpEntity.class);

    @BeforeEach
    public void setup() throws Exception {
        when(_objectData.getLogger()).thenReturn(_logger);
        when(_objectData.getData()).thenReturn(StreamUtil.EMPTY_STREAM);
        when(_objectData.getDataSize()).thenReturn(0L);
        when(_updateRequest.iterator()).thenReturn(Collections.emptyIterator());

        when(_connection.getConnectionProperties()).thenReturn(_connectionProperties);
        when(_connection.getOperationProperties()).thenReturn(_operationProperties);
        when(_connection.getRequestHandler()).thenReturn(_handler);
        when(_response.getEntity()).thenReturn(_entity);
    }

    @Test
    public void shouldAddApplicationErrorForLargeDataHeader_BULK() throws Exception {
        int characterLimit = 1_000_000;
        byte[] bytes = new byte[characterLimit + 1];
        Arrays.fill(bytes, (byte) 'x');

        InputStream xmlInput = new ByteArrayInputStream(bytes);

        when(_entity.getContent()).thenReturn(xmlInput);
        when(_handler.getBulkQueryResult(any(), any(), any())).thenReturn(_response);
        BulkV2QueryReader reader = new BulkV2QueryReader(_connection, "", 2);
        try {
            Exception exception = assertThrows(ConnectorException.class, () -> reader.initQueryResult("", 2L));
            assertEquals(BULK_RESPONSE_CHARS_LIMIT_EXCEEDED_ERROR, exception.getMessage());
        } finally {
            IOUtil.closeQuietly(xmlInput, reader);
        }
    }

    @Test
    public void shouldAddApplicationErrorForLargeDataRecords_BULK() throws Exception {
        ByteArrayOutputStream temp = new ByteArrayOutputStream();
        temp.write("a,b".getBytes(StringUtil.UTF8_CHARSET));
        temp.write('\n');
        for (int i = 0; i < 500000 + 8192 - 3; ++i) // OpenCSV Buffer Size 8192
        {
            temp.write('u');
        }
        temp.write(',');
        for (int i = 0; i < 500000; ++i) {
            temp.write('v');
        }

        ByteArrayInputStream xmlInput = new ByteArrayInputStream(temp.toByteArray());
        IOUtil.closeQuietly(temp);

        when(_entity.getContent()).thenReturn(xmlInput);
        when(_handler.getBulkQueryResult(any(), any(), any())).thenReturn(_response);

        BulkV2QueryReader reader = new BulkV2QueryReader(_connection, "", 2);
        try {
            reader.initQueryResult("", 2L);

            Exception exception = assertThrows(ConnectorException.class, reader::hasNext);
            assertEquals(CSV_CHAR_LIMIT_EXCEEDED_ERROR, exception.getMessage());
        } finally {
            IOUtil.closeQuietly(xmlInput, reader);
        }
    }

    @Test
    public void shouldAcceotSmallDataRecords_BULK() throws Exception {
        ByteArrayOutputStream temp = new ByteArrayOutputStream();
        temp.write("a,b".getBytes(StringUtil.UTF8_CHARSET));
        temp.write('\n');
        for (int i = 0; i < 50000; ++i) // OpenCSV Buffer Size 8192
        {
            temp.write('u');
        }
        temp.write(',');
        for (int i = 0; i < 50000; ++i) {
            temp.write('v');
        }

        ByteArrayInputStream xmlInput = new ByteArrayInputStream(temp.toByteArray());
        IOUtil.closeQuietly(temp);

        when(_entity.getContent()).thenReturn(xmlInput);
        when(_handler.getBulkQueryResult(any(), any(), any())).thenReturn(_response);

        BulkV2QueryReader reader = new BulkV2QueryReader(_connection, "", 2);

        try {
            reader.initQueryResult("", 2L);
            while (reader.hasNext()) {
                Payload x = reader.getNext();
                assertNotNull(x);
            }
        } finally {
            IOUtil.closeQuietly(xmlInput, reader);
        }
    }
}
