// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.operations;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.snowflake.util.TestConfig;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author shaswatadasgupta.
 */
public class SnowflakeCommandsCopyToOptionTest extends BaseTestOperation {

    private static final String OPERATION_NAME = "copyIntoTable";
    private static final String TABLE_NAME = "EMP";
    private List<InputStream> _inputs = null;
    private static final int DOC_COUNT = 2;
    private static final String DB_VALUE = "TEST_SF_DB";
    private static final String FILES = "files";
    private static final String COLUMN_NAMES = "columns";
    private SnowflakeConnection sfConnection;

    @Before
    public void setup() {
        createDatabaseAndSchema();
        testContext = new SnowflakeContextIT(OperationType.EXECUTE, OPERATION_NAME);
        testContext.setObjectTypeId(TABLE_NAME);
        setContext();
        setInputs(StreamUtil.EMPTY_STREAM);
        sfConnection=new SnowflakeConnection(testContext);

    }
    /**
     * Tests if exception is thrown when Snowflake fails to run the COPY INTO command
     */

    @Test
    public void testSnowflakeCommandOperationOverridingDOPWithSchemaAndDB() {
        SnowflakeCommandOperation snowflakeCommandOperation = new SnowflakeCommandOperation(sfConnection);

        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(
                DB_VALUE,
                "DEV");
        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0),
                null, null, dynamicOpProps);

        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);

        snowflakeCommandOperation.executeUpdate(request, response);

        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
    }

    /**
     * Tests if exception is thrown when Snowflake connector fails to connect when db and schema is empty
     */

    @Test
    public void testSnowflakeCommandOperationOverridingDOPWithEmptySchemaAndDB() {
        SnowflakeCommandOperation snowflakeCommandOperation = new SnowflakeCommandOperation(sfConnection);

        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(
                StringUtil.EMPTY_STRING,
                StringUtil.EMPTY_STRING);
        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0),
                null, null, dynamicOpProps);

        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);

        snowflakeCommandOperation.executeUpdate(request, response);

        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
    }

    /**
     * Tests if exception is thrown when Snowflake connector fails to connect to the DB when schema is empty
     */
    @Test
    public void testSnowflakeCommandOperationOverridingDOPWithEmptySchemaAndValidDB() {
        SnowflakeCommandOperation snowflakeCommandOperation = new SnowflakeCommandOperation(sfConnection);

        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(
                DB_VALUE,
                StringUtil.EMPTY_STRING);
        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0),
                null, null, dynamicOpProps);

        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);

        snowflakeCommandOperation.executeUpdate(request, response);

        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
    }

    /**
     * Tests if exception is thrown when Snowflake connector fails to connect when schema is valid
     * but db is empty
     */
    @Test
    public void testSnowflakeCommandOperationOverridingDOPWithValidSchemaAndEmptyDB() {
        SnowflakeCommandOperation snowflakeCommandOperation = new SnowflakeCommandOperation(sfConnection);

        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(
                StringUtil.EMPTY_STRING,
                "DEF");

        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0),
                null, null, dynamicOpProps);

        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);

        snowflakeCommandOperation.executeUpdate(request, response);

        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
    }


    /**
     * Sets the context to connect to the in memory DB.
     */
    private void setContext() {
        testContext.addConnectionProperty("connectionString", "jdbc:h2:mem:TEST_SF_DB");
        testContext.addConnectionProperty("user", System.getProperty("UserName", "sa"));
        testContext.addConnectionProperty("password", System.getProperty("Password", "pwd"));
        testContext.addConnectionProperty("db", "\"TEST_SF_DB\"");
        testContext.addConnectionProperty("schema", "DEV");
        testContext.addConnectionProperty("enablePooling", false);
        testContext.addOperationProperty(COLUMN_NAMES, "ID,DEPT,NAME");
        testContext.addOperationProperty(FILES, "file1.csv,file2.csv");
    }

    /**
     * Sets the input streams for testing.
     * <p>
     * This method initializes the _inputs ArrayList with a specified number of
     * identical input streams. The number of input streams is determined by the
     * DOC_COUNT constant.
     *
     * @param value The InputStream to be repeated in the _inputs list.
     */
    private void setInputs(InputStream value) {
        _inputs = new ArrayList<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            _inputs.add(value);
        }
    }

    /**
     * Creates a dynamic property map with the given database and schema values.
     *
     * @param db     the database value
     * @param schema the schema value
     * @return the created dynamic property map
     */
    private static MutableDynamicPropertyMap createDynamicPropertyMap(String db, String schema) {
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.DATABASE, db);
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.SCHEMA, schema);
        return dynamicOpProps;
    }

    /**
     * Creates a DB and schema using an in memory DB.
     */
    private void createDatabaseAndSchema() {
        TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
        TestConfig.createDbAndSchema("DEF", "EMPLOYEES");
    }


}