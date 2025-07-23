// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert;

import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class JsonQueryFactoryTest {

    private MutableDynamicPropertyMap _properties;

    @Test
    public void shouldCreateJsonCorrectly() {
        initializeAllPropertiesValues();
        JsonQueryFactory factory = new JsonQueryFactory(_properties, "proyectIdTest");
        JsonNode query = factory.toJsonNode();
        assertNotNull(query);
        assertQueryNode(query);
    }

    private void assertQueryNode(JsonNode query) {
        JsonNode node = query.path("configuration").path("query").path("query");
        String queryNode = node.asText();
        assertFalse(queryNode.contains("$QUERY_TARGET_TABLE"));
        assertFalse(queryNode.contains("$LOAD_TARGET_TABLE"));
    }

    private void initializeAllPropertiesValues() {
        _properties = new MutableDynamicPropertyMap();
        _properties.addProperty("useLegacySql", "true");
        _properties.addProperty("sqlCommand", "MERGE $QUERY_TARGET_TABLE TDST USING $LOAD_TARGET_TABLE TSRC ");
        _properties.addProperty("datasetId", "datasetTest");
        _properties.addProperty("targetTableForQuery", "queryTargetTest");
        _properties.addProperty("temporaryTableForLoad", "loadTargetTest");
    }
}