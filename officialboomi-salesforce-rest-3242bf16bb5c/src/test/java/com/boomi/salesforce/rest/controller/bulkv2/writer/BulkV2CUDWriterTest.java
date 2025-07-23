// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2.writer;

import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.model.SObjectField;
import com.boomi.salesforce.rest.model.SObjectModel;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkV2CUDWriterTest {

    private static final String EXPECTED_CSV_HEADER = "\"Id\",\"UnitPrice\",\"IsActive\",\"UseStandardPrice\"\n";
    private static final String EXPECTED_CSV_ROW = "\"01uHp00000Ohp2sIAB\",\"5001.01\",\"true\",\"\"\n";
    private static final String XML_CONTENT =
            "<records type=\"PricebookEntry\"><Id>01uHp00000Ohp2sIAB</Id><UnitPrice>5001"
            + ".01</UnitPrice><IsActive>true</IsActive></records>";

    private SFRestConnection _connectionMock;

    @BeforeEach
    void setup() {
        _connectionMock = mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);
        when(_connectionMock.getConnectionProperties().getSObject(any(), any(), anyBoolean())).thenReturn(buildModel());
        when(_connectionMock.getConnectionProperties().isSObjectCached(any(), any(), anyBoolean())).thenReturn(true);
        when(_connectionMock.getOperationProperties().getSObject()).thenReturn("PricebookEntry");
    }

    @Test
    void receive() throws IOException {
        when(_connectionMock.getOperationProperties().getOperationBoomiName()).thenReturn("UPSERT");

        BulkV2CUDWriter writer = new BulkV2CUDWriter(_connectionMock);
        writer.init();

        writer.receive(new ByteArrayInputStream(XML_CONTENT.getBytes(StringUtil.UTF8_CHARSET)));

        InputStream result = writer.getInputStream();

        assertEquals(EXPECTED_CSV_HEADER + EXPECTED_CSV_ROW, StreamUtil.toString(result, StringUtil.UTF8_CHARSET));
        assertEquals((EXPECTED_CSV_HEADER + EXPECTED_CSV_ROW).length(), writer.getContentLength());
    }

    @Test
    void csvHeaderTest() throws IOException {
        BulkV2CUDWriter writer = new BulkV2CUDWriter(_connectionMock);
        writer.init();

        InputStream result = writer.getInputStream();
        assertEquals(EXPECTED_CSV_HEADER, StreamUtil.toString(result, StringUtil.UTF8_CHARSET));
        assertEquals(EXPECTED_CSV_HEADER.length(), writer.getContentLength());
    }

    @Test
    void receiveDelete() throws IOException {
        when(_connectionMock.getOperationProperties().getBulkHeader()).thenReturn("id");

        BulkV2CUDWriter writer = new BulkV2CUDWriter(_connectionMock);
        writer.init();

        writer.receiveDelete("theIdToBeDeleted");

        InputStream result = writer.getInputStream();

        final String expectedCSV = "\"id\"\n\"theIdToBeDeleted\"\n";
        assertEquals(expectedCSV, StreamUtil.toString(result, StringUtil.UTF8_CHARSET));
        assertEquals(expectedCSV.length(), writer.getContentLength());
    }

    private static SObjectModel buildModel() {
        List<SObjectField> fields = new ArrayList<>();
        fields.add(new SObjectField("Id", "id"));
        fields.add(new SObjectField("UnitPrice", "currency", 18, 2));
        fields.add(new SObjectField("IsActive", "boolean"));
        fields.add(new SObjectField("UseStandardPrice", "boolean"));
        return new SObjectModel(fields);
    }
}
