// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.fields;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Factory class to handle the creation of Browse Fields
 */
public class ImportableFieldsFactory {

    private static final Logger LOG = LogUtil.getLogger(ImportableFieldsFactory.class);

    private static final String ERROR_NOT_SCHEMA =
            "Unable to pretty print the JSON schema. Will just output it using the toString() method";
    private static final String MERGE_USING_CONDITION = "MERGE $QUERY_TARGET_TABLE TDST USING $LOAD_TARGET_TABLE TSRC ";
    private static final String WHEN_MATCHED_CONDITION = " WHEN MATCHED THEN UPDATE SET ";
    private static final String NOT_MATCH = " WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)";
    private static final String QUERY_ALIAS_CONDITION = "TDST.%s = TSRC.%s";
    private static final String VALUES_ALIAS_CONDITION = "TSRC.%s";
    private static final String COMMA = ", ";
    private static final String AND = " AND ";
    private static final String NODE_SCHEMA = "schema";
    private static final Pattern COMPILE = Pattern.compile("-");
    private static final String UNDER_SCORE = "_";

    private final GoogleBqBaseConnection<BrowseContext> _conn;

    public ImportableFieldsFactory(GoogleBqBaseConnection<BrowseContext> conn) {
        _conn = conn;
    }

    /**
     * Create a collection of {@link BrowseField} with pre default values if the Object Type is not DYNAMIC, otherwise
     * the values are empty.
     *
     * @param objectTypeId
     * @return a list of Browse Field
     */
    public Collection<BrowseField> importableFields(String objectTypeId) {
        Collection<BrowseField> importableFields = new ArrayList<>();

        String defaultDestinationTableForLoad = null;
        String defaultTemporaryTableForLoad = null;
        String defaultTableSchema = null;
        String defaultSqlCommand = null;
        String defaultTargetTableForQuery = null;

        if (!GoogleBqConstants.GENERIC_TABLE_ID.equals(objectTypeId)) {
            String tableName = extractTableName(objectTypeId);
            defaultTemporaryTableForLoad = generateTemporaryRandomTableName(tableName);
            defaultDestinationTableForLoad = tableName;
            defaultTargetTableForQuery = tableName;
            try {
                JsonNode schema = getSchemaNodeFromObjectType(_conn, objectTypeId);
                defaultTableSchema = JSONUtil.prettyPrintJSON(schema);
                defaultSqlCommand = buildTemplateQuery(schema);
            } catch (JsonProcessingException e) {
                LOG.log(Level.INFO, ERROR_NOT_SCHEMA, e);
            }
        }

        importableFields.add(ImportableField.DESTINATION_TABLE_FOR_LOAD.toBrowseField(defaultDestinationTableForLoad));
        importableFields.add(ImportableField.TEMPORARY_TABLE_FOR_LOAD.toBrowseField(defaultTemporaryTableForLoad));
        importableFields.add(ImportableField.TABLE_SCHEMA.toBrowseField(defaultTableSchema));
        importableFields.add(ImportableField.RUN_SQL_AFTER_LOAD.toBrowseField());
        importableFields.add(ImportableField.USE_LEGACY_SQL.toBrowseField());
        importableFields.add(ImportableField.SQL_COMMAND.toBrowseField(defaultSqlCommand));
        importableFields.add(ImportableField.TARGET_TABLE_FOR_QUERY.toBrowseField(defaultTargetTableForQuery));
        importableFields.add(ImportableField.DELETE_TEMP_TABLE_AFTER_QUERY.withAllowedValues());
        return importableFields;
    }

    private static String generateTemporaryRandomTableName(String tableName) {
        return tableName + UNDER_SCORE + COMPILE.matcher(UUID.randomUUID().toString()).replaceAll(UNDER_SCORE);
    }

    /**
     * Returns a {@link JsonNode} with the schema of the table selected as object type. {@link
     * TableResource#getTable(String)} returns the metadata of a table.
     *
     * @param objectTypeId
     * @return
     */
    private static JsonNode getSchemaNodeFromObjectType(GoogleBqBaseConnection<BrowseContext> conn,
            String objectTypeId) {
        TableResource tableResource = new TableResource(conn);
        JsonNode node;
        try {
            node = tableResource.getTable(objectTypeId);
        } catch (IOException | GeneralSecurityException e) {
            throw new ConnectorException("Unable to generate schema for the table.", e);
        }
        JsonNode schema = node.path(NODE_SCHEMA);

        if (schema.isMissingNode()) {
            throw new ConnectorException("Unable to retrieve schema for the table. No schema found");
        }
        return schema;
    }

    /**
     * Returns a {@link String} with an auto-generate name using the Object type as Input and append a random {@link
     * UUID}
     *
     * @param objectTypeId
     * @return
     */
    private static String extractTableName(String objectTypeId) {
        return objectTypeId.substring(objectTypeId.lastIndexOf('/') + 1);
    }

    /**
     * Creates an SQL Template from the {@link JsonNode} schema
     *
     * @param schema
     * @return A String with a default SQL template
     */
    private static String buildTemplateQuery(JsonNode schema) {
        List<String> fieldNames = extractFieldNames(schema);
        List<String> aliasCondition = fieldAliasCondition(fieldNames);

        String onCondition = "ON " + StringUtil.join(AND, aliasCondition);
        String matchCondition = WHEN_MATCHED_CONDITION + StringUtil.join(COMMA, aliasCondition);
        String joinCommaFields = StringUtil.join(COMMA, fieldNames);
        String aliasFieldsCondition = buildAliasMatchCondition(fieldNames);
        String noMatchCondition = String.format(NOT_MATCH, joinCommaFields, aliasFieldsCondition);

        return MERGE_USING_CONDITION + onCondition + matchCondition + noMatchCondition;
    }

    private static String buildAliasMatchCondition(Collection<String> fieldNames) {
        Collection<String> fields = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fields.add(String.format(VALUES_ALIAS_CONDITION, fieldName));
        }
        return StringUtil.join(COMMA, fields);
    }

    private static List<String> fieldAliasCondition(Collection<String> fieldNames) {
        List<String> fields = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fields.add(String.format(QUERY_ALIAS_CONDITION, fieldName, fieldName));
        }
        return fields;
    }

    private static List<String> extractFieldNames(JsonNode schema) {
        List<String> fields = new ArrayList<>();
        for (JsonNode field : schema.path("fields")) {
            String fieldName = field.path("name").asText();
            fields.add(fieldName);
        }
        return fields;
    }
}
