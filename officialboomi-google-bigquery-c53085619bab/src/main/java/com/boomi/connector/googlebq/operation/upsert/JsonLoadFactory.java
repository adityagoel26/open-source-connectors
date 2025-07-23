// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/**
 * This class is responsible for creating a JSON structure using the parameters of the operation, to be processed as a
 * Load Job in Big Query.
 */
public class JsonLoadFactory extends JsonJobFactory {

    private static final String ALLOW_JAGGED_ROWS_FIELD = "allowJaggedRows";
    private static final String SOURCE_FORMAT_FIELD = "sourceFormat";
    private static final String WRITE_DISPOSITION_FIELD = "writeDisposition";
    private static final String CREATE_DISPOSITION_FIELD = "createDisposition";
    private static final String ENCODING_FIELD = "encoding";
    private static final String DESTINATION_TABLE = "destinationTable";
    private static final String MAX_BAD_RECORDS_FIELD = "maxBadRecords";
    private static final String AUTODETECT_FIELD = "autodetect";
    private static final String SCHEMA_FIELD = "schema";
    private static final String TABLE_ID_FIELD = "tableId";
    private static final String DELIMITER_FIELD = "fieldDelimiter";
    private static final String SKIP_LEADING_ROWS_FIELD = "skipLeadingRows";
    private static final String QUOTE_FIELD = "quote";
    private static final String NULL_MARKER_FIELD = "nullMarker";
    private static final String ALLOW_QUOTED_NEWLINES_FIELD = "allowQuotedNewlines";
    private static final String ENUM_AS_STRING_FIELD = "enumAsString";
    private static final String LIST_INFERENCE_FIELD = "enableListInference";
    private static final String AVRO_LOGICAL_TYPE_FIELD = "useAvroLogicalTypes";
    private static final String MAX_BAD_RECORDS_CSV = "maxBadRecordsCSV";
    private static final String AUTODETECT_CSV = "autodetectCSV";
    private static final String AUTODETECT_JSON = "autodetectJSON";
    private static final String MAX_BAD_RECORDS_JSON = "maxBadRecordsJSON";
    private static final String DATASET_ID_FIELD = "datasetId";
    private static final String RUN_SQL_AFTER_LOAD_FIELD = "runSqlAfterLoad";
    private static final String TEMPORARY_TABLE_FOR_LOAD_FIELD = "temporaryTableForLoad";
    private static final String DESTINATION_TABLE_FOR_LOAD_FIELD = "destinationTableForLoad";
    private static final String TABLE_SCHEMA_FIELD = "tableSchema";
    private static final String LOAD_FIELD = "load";

    private final DynamicPropertyMap _properties;

    public JsonLoadFactory(DynamicPropertyMap properties, String projectId) {
        super(projectId);
        _properties = properties;
    }

    private enum SourceType {
        CSV, NEWLINE_DELIMITED_JSON, PARQUET, AVRO, ORC
    }

    @Override
    public JsonNode createConfigurationNode() {
        ObjectNode objectNode = createJsonFromSourceType();
        ObjectNode configurationNode = JSONUtil.newObjectNode();
        configurationNode.set(LOAD_FIELD, objectNode);
        return configurationNode;
    }

    private ObjectNode createJsonFromSourceType() {
        ObjectNode objectNode = JSONUtil.newObjectNode();
        addPropertyToJsonNode(objectNode, WRITE_DISPOSITION_FIELD);
        addPropertyToJsonNode(objectNode, CREATE_DISPOSITION_FIELD);
        addPropertyToJsonNode(objectNode, ENCODING_FIELD);
        addPropertyToJsonNode(objectNode, SOURCE_FORMAT_FIELD);
        objectNode.set(DESTINATION_TABLE, buildDestinationTableNode());

        addSourceFormatAttributes(objectNode);
        return objectNode;
    }

    private ObjectNode buildDestinationTableNode() {
        String datasetId = getPropertyValue(DATASET_ID_FIELD);

        ObjectNode destTableNode = JSONUtil.newObjectNode();
        destTableNode.put(PROJECT_ID_FIELD, _projectId);
        destTableNode.put(DATASET_ID_FIELD, datasetId);

        boolean runSqlAfterLoad = geBooleanPropertyValue(RUN_SQL_AFTER_LOAD_FIELD);

        String tableName = runSqlAfterLoad ? TEMPORARY_TABLE_FOR_LOAD_FIELD : DESTINATION_TABLE_FOR_LOAD_FIELD;
        destTableNode.put(TABLE_ID_FIELD, getPropertyValue(tableName));

        return destTableNode;
    }

    private void addSourceFormatAttributes(ObjectNode objectNode) {
        String sourceFormat = getPropertyValue(SOURCE_FORMAT_FIELD);
        switch (SourceType.valueOf(sourceFormat)) {
            case CSV:
                boolean autodetectCsv = geBooleanPropertyValue(AUTODETECT_CSV);
                if (!autodetectCsv) {
                    addSchemaPropertyToJsonNode(objectNode);
                }
                addPropertyToJsonNode(objectNode, DELIMITER_FIELD);
                addPropertyToJsonNode(objectNode, SKIP_LEADING_ROWS_FIELD);
                addPropertyToJsonNode(objectNode, QUOTE_FIELD);
                addPropertyToJsonNode(objectNode, NULL_MARKER_FIELD);
                addPropertyToJsonNode(objectNode, MAX_BAD_RECORDS_FIELD, MAX_BAD_RECORDS_CSV);
                addBooleanPropertyToJsonNode(objectNode, ALLOW_QUOTED_NEWLINES_FIELD);
                addBooleanPropertyToJsonNode(objectNode, ALLOW_JAGGED_ROWS_FIELD);
                addBooleanPropertyToJsonNode(objectNode, AUTODETECT_FIELD, AUTODETECT_CSV);
                break;
            case NEWLINE_DELIMITED_JSON:
                boolean autodetectJson = geBooleanPropertyValue(AUTODETECT_JSON);
                if (!autodetectJson) {
                    addSchemaPropertyToJsonNode(objectNode);
                }
                addBooleanPropertyToJsonNode(objectNode, AUTODETECT_FIELD, AUTODETECT_JSON);
                addPropertyToJsonNode(objectNode, MAX_BAD_RECORDS_FIELD, MAX_BAD_RECORDS_JSON);
                break;
            case PARQUET:
                addBooleanPropertyToJsonNode(objectNode, ENUM_AS_STRING_FIELD);
                addBooleanPropertyToJsonNode(objectNode, LIST_INFERENCE_FIELD);
                break;
            case AVRO:
                addBooleanPropertyToJsonNode(objectNode, AVRO_LOGICAL_TYPE_FIELD);
                break;
            case ORC:
                break;
            default:
                throw new IllegalArgumentException("Invalid source type " + sourceFormat);
        }
    }

    private void addPropertyToJsonNode(ObjectNode objectNode, String fieldName) {
        addPropertyToJsonNode(objectNode, fieldName, fieldName);
    }

    private void addPropertyToJsonNode(ObjectNode objectNode, String jsonAttrName, String fieldName) {
        String value = getPropertyValue(fieldName);
        if (StringUtil.isNotEmpty(value)) {
            objectNode.put(jsonAttrName, value);
        }
    }

    private void addSchemaPropertyToJsonNode(ObjectNode objectNode) {
        String value = getPropertyValue(TABLE_SCHEMA_FIELD);
        if (StringUtil.isNotEmpty(value)) {
            try {
                objectNode.set(SCHEMA_FIELD, JSONUtil.getDefaultObjectMapper().readTree(value));
            } catch (IOException e) {
                throw new ConnectorException("Error reading schema ", e);
            }
        }
    }

    private void addBooleanPropertyToJsonNode(ObjectNode objectNode, String jsonAtrrName) {
        addBooleanPropertyToJsonNode(objectNode, jsonAtrrName, jsonAtrrName);
    }

    private void addBooleanPropertyToJsonNode(ObjectNode objectNode, String jsonAttrName, String fieldName) {
        boolean value = geBooleanPropertyValue(fieldName);
        objectNode.put(jsonAttrName, value);
    }

    private String getPropertyValue(String name) {
        return _properties.getProperty(name);
    }

    private boolean geBooleanPropertyValue(String name) {
        return Boolean.parseBoolean(getPropertyValue(name));
    }
}