// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This class is responsible for creating a JSON structure using the parameters of the operation, to be processed as a
 * Load Job in Big Query.
 */
public class JsonQueryFactory extends JsonJobFactory {

    private static final String QUERY_TARGET_TOKEN = "$QUERY_TARGET_TABLE";
    private static final String LOAD_TARGET_TOKEN = "$LOAD_TARGET_TABLE";
    private static final String DATASET_TABLE_PREFIX = "%s.%s.%s";
    private static final String QUERY_FIELD = "query";
    private static final String USE_LEGACY_SQL = "useLegacySql";
    private static final String SQL_COMMAND = "sqlCommand";
    private static final String TARGET_TABLE_FOR_QUERY = "targetTableForQuery";
    private static final String TEMPORARY_TABLE_FOR_LOAD = "temporaryTableForLoad";
    private static final String DATASET_ID = "datasetId";

    private final DynamicPropertyMap _properties;
    private final String _datasetId;

    public JsonQueryFactory(DynamicPropertyMap properties, String projectId) {
        super(projectId);
        _properties = properties;
        _datasetId = _properties.getProperty(DATASET_ID);
    }

    @Override
    public JsonNode createConfigurationNode() {
        ObjectNode objectNode = createQueryNode();
        ObjectNode configurationNode = JSONUtil.newObjectNode();
        configurationNode.set(QUERY_FIELD, objectNode);

        return configurationNode;
    }

    private ObjectNode createQueryNode() {
        ObjectNode queryNode = JSONUtil.newObjectNode();
        queryNode.put(QUERY_FIELD, buildQuery());

        boolean useLegacySql = Boolean.parseBoolean(_properties.getProperty(USE_LEGACY_SQL));
        queryNode.put(USE_LEGACY_SQL, useLegacySql);

        return queryNode;
    }

    private String buildQuery() {
        String sqlCommand = _properties.getProperty(SQL_COMMAND);

        if (StringUtil.isEmpty(sqlCommand)) {
            throw new ConnectorException("SQL Command field cannot be empty");
        }

        sqlCommand = replaceSQLToken(sqlCommand, QUERY_TARGET_TOKEN, TARGET_TABLE_FOR_QUERY);
        sqlCommand = replaceSQLToken(sqlCommand, LOAD_TARGET_TOKEN, TEMPORARY_TABLE_FOR_LOAD);

        return sqlCommand;
    }

    private String replaceSQLToken(String sqlCommand, String token, String propertyName) {
        String tableName = _properties.getProperty(propertyName);

        if (StringUtil.isNotBlank(tableName)) {
            sqlCommand = StringUtil.fastReplace(sqlCommand, token,
                    String.format(DATASET_TABLE_PREFIX, _projectId, _datasetId, tableName));
        }
        return sqlCommand;
    }
}