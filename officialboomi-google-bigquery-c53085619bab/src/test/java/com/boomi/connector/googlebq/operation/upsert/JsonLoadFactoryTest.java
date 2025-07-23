// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert;

import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JsonLoadFactoryTest {

    private MutableDynamicPropertyMap _properties;
    private JsonLoadFactory _loadFactory;
    private static final String MAX_BAD_RECORDS_FIELD = "maxBadRecords";
    private static final String AUTODETECT_FIELD = "autodetect";

    @Before
    public void setUp() {
        initializeAllPropertiesValues();
        _loadFactory = new JsonLoadFactory(_properties, "proyectIdTest");
    }

    @Test
    public void shouldCreateJsonWithCSVCorrectly() {
        String fieldDelimiter = "fieldDelimiter";
        String fieldDelimiterValue = "|";
        String encoding = "encoding";
        String encodingValue = "UTF-8";
        String sourceFormat = "sourceFormat";
        String sourceFormatValue = "CSV";
        String skipLeadingRows = "skipLeadingRows";
        String skipLeadingRowsValue = "1";
        String quote = "quote";
        String quoteValue = "\"";
        String nullMarker = "nullMarker";
        String nullMarkerValue = "null";
        String maxBadRecordsCSV = "maxBadRecordsCSV";
        String maxBadRecordsCSVValue = "1";
        String allowQuotedNewlines = "allowQuotedNewlines";
        String allowQuotedNewlinesValue = "true";
        String allowJaggedRows = "allowJaggedRows";
        String allowJaggedRowsValue = "true";

        _properties.addProperty(encoding, encodingValue);
        _properties.addProperty(sourceFormat, sourceFormatValue);
        _properties.addProperty(fieldDelimiter, fieldDelimiterValue);
        _properties.addProperty(skipLeadingRows, skipLeadingRowsValue);
        _properties.addProperty(quote, quoteValue);
        _properties.addProperty(nullMarker, nullMarkerValue);
        _properties.addProperty(maxBadRecordsCSV, maxBadRecordsCSVValue);
        _properties.addProperty(allowQuotedNewlines, allowQuotedNewlinesValue);
        _properties.addProperty(allowJaggedRows, allowJaggedRowsValue);
        _properties.addProperty("runSqlAfterLoad", "true");

        JsonNode load = _loadFactory.toJsonNode();
        assertNotNull(load);
        assertNode(load, fieldDelimiter, fieldDelimiterValue);
        assertNode(load, encoding, encodingValue);
        assertNode(load, sourceFormat, sourceFormatValue);
        assertNode(load, skipLeadingRows, skipLeadingRowsValue);
        assertNode(load, quote, quoteValue);
        assertNode(load, nullMarker, nullMarkerValue);
        assertNode(load, MAX_BAD_RECORDS_FIELD, maxBadRecordsCSVValue);
        assertNode(load, allowQuotedNewlines, allowQuotedNewlinesValue);
        assertNode(load, allowJaggedRows, allowJaggedRowsValue);
    }

    @Test
    public void shouldCreateJsonWithJSONCorrectly() {
        String maxBadRecordsJSON = "maxBadRecordsJSON";
        String maxBadRecordsJSONValue = "1";
        String autodetectJSON = "autodetectJSON";
        String autodetectJSONValue = "true";

        _properties.addProperty("runSqlAfterLoad", "false");
        _properties.addProperty("sourceFormat", "NEWLINE_DELIMITED_JSON");
        _properties.addProperty(maxBadRecordsJSON, maxBadRecordsJSONValue);
        _properties.addProperty(autodetectJSON, autodetectJSONValue);

        JsonNode load = _loadFactory.toJsonNode();
        assertNotNull(load);
        assertNode(load, MAX_BAD_RECORDS_FIELD, maxBadRecordsJSONValue);
        assertNode(load, AUTODETECT_FIELD, autodetectJSONValue);
    }

    @Test
    public void shouldCreateJsonWithPARQUETCorrectly() {
        String enumAsString = "enumAsString";
        String enumAsStringValue = "true";
        String listInference = "enableListInference";
        String listInferenceValue = "true";

        _properties.addProperty("sourceFormat", "PARQUET");
        _properties.addProperty(enumAsString, enumAsStringValue);
        _properties.addProperty(listInference, listInferenceValue);
        JsonNode load = _loadFactory.toJsonNode();
        assertNotNull(load);
        assertNode(load, enumAsString, enumAsStringValue);
        assertNode(load, listInference, listInferenceValue);
    }

    @Test
    public void shouldCreateJsonWithAVROCorrectly() {
        String avroLogicalType = "useAvroLogicalTypes";
        String avroLogicalTypeValue = "true";

        _properties.addProperty("sourceFormat", "AVRO");
        _properties.addProperty(avroLogicalType, avroLogicalTypeValue);
        JsonNode load = _loadFactory.toJsonNode();
        assertNotNull(load);
        assertNode(load, avroLogicalType, avroLogicalTypeValue);
    }

    @Test
    public void shouldCreateJsonWithORCCorrectly() {
        _properties.addProperty("sourceFormat", "ORC");
        JsonNode load = _loadFactory.toJsonNode();
        assertNotNull(load);
    }

    private void assertNode(JsonNode load, String fieldName, String actualField) {
        JsonNode node = load.path("configuration").path("load").path(fieldName);
        assertEquals(actualField, node.asText());
    }

    private void initializeAllPropertiesValues() {
        _properties = new MutableDynamicPropertyMap();
        _properties.addProperty("writeDisposition", "writeDisposition");
        _properties.addProperty("createDisposition", "createDisposition ");
        _properties.addProperty("temporaryTableForLoad", "loadTargetTest");
        _properties.addProperty("datasetId", "dataset");
        _properties.addProperty("tableSchema",
                "{\"fields\": [{\"name\": \"string_field_0\",\"type\": \"STRING\"," + "\"mode\": \"NULLABLE\"}]}");
    }
}