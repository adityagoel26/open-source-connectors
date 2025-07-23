// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.mongodb;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.mongodb.util.QueryOperationUtil;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Enclosed.class)
public class MongoDBQueryOperationTest {

    private static final OperationContext operationContext = mock(OperationContext.class);
    private static final MongoDBConnectorConnection connectorConnection = mock(MongoDBConnectorConnection.class);
    private static final MongoDBConnectorQueryOperation mongoDBConnectorQueryOperation = new MongoDBConnectorQueryOperation(
            connectorConnection);

    private MongoDBQueryOperationTest() {

    }
    @RunWith(Parameterized.class)
    public static class MongoDBQueryOperationsParameterizedTest {

        private final String modifiedPropName;
        private final Object formattedParamValue;
        private final Bson expectedFilter;
        private QueryOperationUtil queryOperationUtil;

        public MongoDBQueryOperationsParameterizedTest(QueryOperationUtil queryOperationUtil, String modifiedPropName,
                Object formattedParamValue, Bson expectedFilter) {
            this.queryOperationUtil = queryOperationUtil;
            this.modifiedPropName = modifiedPropName;
            this.formattedParamValue = formattedParamValue;
            this.expectedFilter = expectedFilter;
        }

        @Parameterized.Parameters(name = "propName #{0} with Value {1}")
        public static Collection<Object[]> getParametersQueryCondition() {
            return Arrays.asList(new Object[][] {
                    { QueryOperationUtil.EQUALS, "propName", "Value", Filters.eq("propName", "Value") },
                    {QueryOperationUtil.NOT_EQUALS, "propName1", "Value1",Filters.not(Filters.eq("propName1",
                            "Value1")) },
                    { QueryOperationUtil.GREATER_THAN, "propName2", 10, Filters.gt("propName2", 10) },
                    { QueryOperationUtil.LESS_THAN, "propName3", 20, Filters.lt("propName3", 20) },
                    { QueryOperationUtil.GREATER_THAN_OR_EQUALS, "propName4", 25, Filters.gte("propName4", 25) },
                    { QueryOperationUtil.LESS_THAN_OR_EQUALS, "propName5", 30, Filters.lte("propName5", 30) },
                    {QueryOperationUtil.IN_LIST, "propName6", Arrays.asList("value2", "value3"),
                            Filters.in("propName6", Arrays.asList("value2", "value3")) } });
        }

        @Test
        public void testQueryCondition() {

            Bson actualFilter = mongoDBConnectorQueryOperation.queryCondition(queryOperationUtil, modifiedPropName,
                    formattedParamValue);

            assertEquals(expectedFilter, actualFilter);
        }
    }

    public static class MongoDBQueryOperationsNonParameterizedTest {

        @Test
        public void testConnection() {
            MongoDBConnectorConnection mongoDBConnectorConnection = mongoDBConnectorQueryOperation.getConnection();
            assertNotNull(mongoDBConnectorConnection);
            mongoDBConnectorConnection.closeConnection();
        }

        @Test
        public void testGetRecordSchemaForQueryIsNotNullValue() {
            String expectedProfile = "{\"type\": \"object\", "
                    + "\"properties\": {\"_id\": { \"type\": \"object\", \"properties\": {\"$oid\": { \"type\": "
                    + "\"string\" }}},\"name\": { \"type\": \"string\" },\"location\": { \"type\": \"string\" },"
                    + "\"email\": { \"type\": \"string\" }}}";

            when(mongoDBConnectorQueryOperation.getContext()).thenReturn(operationContext);
            when(operationContext.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn(
                    "NONElkstrey" + "{\"type\": \"object\", "
                            + "\"properties\": {\"_id\": { \"type\": \"object\", \"properties\": {\"$oid\": { "
                            + "\"type\": "
                            + "\"string\" }}},\"name\": { \"type\": \"string\" },\"location\": { \"type\": \"string\""
                            + " },"
                            + "\"email\": { \"type\": \"string\" }}}");
            String actualProfile = mongoDBConnectorQueryOperation.getRecordSchemaForQuery();

            assertEquals(expectedProfile, actualProfile);
        }

        @Test
        public void testGetRecordSchemaForQueryIsNullValue() {
            String expectedProfile = null;

            when(mongoDBConnectorQueryOperation.getContext()).thenReturn(operationContext);
            when(operationContext.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn("NONElkstreynull");

            String actualProfile = mongoDBConnectorQueryOperation.getRecordSchemaForQuery();

            assertEquals(expectedProfile, actualProfile);
        }
    }
}