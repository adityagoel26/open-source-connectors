// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.operations;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.SnowflakeContextIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Logger;

/**
 * SnowflakeQueryOperationTest for SnowflakeQueryOperation
 */
@RunWith(Parameterized.class)
@PrepareForTest(SnowflakeQueryOperation.class)
public class SnowflakeQueryOperationTest extends BaseTestOperation {

    private static final String TABLE_NAME = "QUERY_OPERATION_TESTER";
    private static final long DEFAULT_BATCH_SIZE = 3;
    private static final String DATABASE = "db";
    private static final String SCHEMA = "schema";
    private QueryRequest mockRequest;
    private OperationResponse mockResponse;
    private SnowflakeQueryOperation snowflakeQueryOperation;
    private FilterData mockFilterData;
    private Connection connectionMock;
    private final String dbDynamicOperationProperty;
    private final String schemaDynamicOperationProperty;

    /**
     * Constructer for DynamicOperationProperty
     */
    public SnowflakeQueryOperationTest(
            String dbDynamicOperationProperty,
            String schemaDynamicOperationProperty) {
        this.dbDynamicOperationProperty = dbDynamicOperationProperty;
        this.schemaDynamicOperationProperty = schemaDynamicOperationProperty;
    }

    /**
     * mock up for ExecuteQuery operation
     */
    @Before
    public void setUp() throws Exception {
        SnowflakeConnection mockSnowflakeConnection = PowerMockito.mock(SnowflakeConnection.class);
        snowflakeQueryOperation = new SnowflakeQueryOperation(mockSnowflakeConnection);
        testContext = new SnowflakeContextIT(OperationType.QUERY, null);
        testContext.setObjectTypeId(TABLE_NAME);
        testContext.setBatchSize(DEFAULT_BATCH_SIZE);

        QueryFilter mockQueryFilter = PowerMockito.mock(QueryFilter.class);
        mockFilterData = PowerMockito.mock(FilterData.class);
        Logger mockLogger = PowerMockito.mock(Logger.class);
        mockRequest = PowerMockito.mock(QueryRequest.class);
        mockResponse = PowerMockito.mock(OperationResponse.class);

        Mockito.when(mockResponse.getLogger()).thenReturn(Logger.getLogger(SnowflakeQueryOperation.class.getName()));
        Mockito.when(mockRequest.getFilter()).thenReturn(mockFilterData);
        Mockito.when(mockFilterData.getFilter()).thenReturn(mockQueryFilter);
        Mockito.when(mockFilterData.getLogger()).thenReturn(mockLogger);
        Mockito.when(mockSnowflakeConnection.getContext()).thenReturn(testContext);
        Mockito.when(mockSnowflakeConnection.getOperationContext()).thenReturn(testContext);

        ConnectionProperties connectionPropertiesMock = PowerMockito.mock(ConnectionProperties.class);
        PowerMockito.whenNew(ConnectionProperties.class)
                .withArguments(mockSnowflakeConnection, testContext.getOperationProperties(), testContext.getObjectTypeId(),
                        mockLogger)
                .thenReturn(connectionPropertiesMock);
        connectionMock = PowerMockito.mock(Connection.class);
        Mockito.when(mockSnowflakeConnection.createJdbcConnection()).thenReturn(connectionMock);
    }

    /**
     * mock up for ExecuteQuery operation
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // Test ExecuteQuery with DynamicOperationProperty DB and SCHEMA - valid value.
                {"DBDynamicOperationPropertyTest1",
                        "SchemaDynamicOperationPropertyTest1"},
                // Test ExecuteQuery with DynamicOperationProperty DB and SCHEMA - DB valid and SCHEMA empty.
                {"DBDynamicOperationPropertyTest1", ""},

                // Test ExecuteQuery with DynamicOperationProperty DB and SCHEMA - DB empty and SCHEMA valid.
                {"", "SchemaDynamicOperationPropertyTest1"},

                // Test ExecuteQuery with DynamicOperationProperty DB and SCHEMA - DB empty and SCHEMA empty.
                {"", ""},

                // Test ExecuteQuery with DynamicOperationProperty DB and SCHEMA - DB and SCHEMA whitespaces.
                {" ", " "},
        });
    }

    /**
     * Test ExecuteQuery with DynamicOperationProperty DB and SCHEMA - test case.
     */
    @Test
    public void testExecuteQuery_With_OverrideDynamicOperationProperty() throws SQLException {
        DynamicPropertyMap dynamicPropertiesMap = createDynamicPropertyMap(dbDynamicOperationProperty, schemaDynamicOperationProperty);

        Mockito.when(mockFilterData.getDynamicOperationProperties()).thenReturn(dynamicPropertiesMap);

        snowflakeQueryOperation.executeQuery(mockRequest, mockResponse);


        //verifying connection is being called with catalog and schema
        Mockito.verify(connectionMock).setCatalog(dbDynamicOperationProperty.toUpperCase());
        Mockito.verify(connectionMock).setSchema(schemaDynamicOperationProperty.toUpperCase());
        }

    /**
     * create createDynamicPropertyMap with DB and Schema property.
     */
    private static DynamicPropertyMap createDynamicPropertyMap(String dbValue, String schemaValue) {
        MutableDynamicPropertyMap dynamicProperty = new MutableDynamicPropertyMap();
        dynamicProperty.addProperty(DATABASE, dbValue);
        dynamicProperty.addProperty(SCHEMA, schemaValue);
        return dynamicProperty;
    }
}
