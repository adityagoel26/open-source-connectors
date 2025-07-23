// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.controllers;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.InMemoryDataBase;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.snowflake.wrappers.SnowflakeTableStream;
import com.boomi.util.StreamUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.logging.Logger;

public class SnowflakeGetControllerTest {

    private ConnectionProperties properties;
    private List<InputStream> inputs;
    private Field readerField;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
        TestConfig.createDbAndSchema("DEF", "EMPLOYEES");
        SnowflakeContextIT testContext = new SnowflakeContextIT(OperationType.GET, "get");
        testContext.addConnectionProperty("connectionString", "jdbc:h2:mem:TEST_SF_DB");
        testContext.addConnectionProperty("user", System.getProperty("UserName", "sa"));
        testContext.addConnectionProperty("password", System.getProperty("Password", "pwd"));
        testContext.addConnectionProperty("db", "\"TEST_SF_DB\"");
        testContext.addConnectionProperty("schema", "DEV");
        testContext.addOperationProperty("stageName", null);
        testContext.addOperationProperty("awsBucketName", null);

        SnowflakeConnection connection = new SnowflakeConnection(testContext);
        properties = new ConnectionProperties(connection,
                new MutablePropertyMap(), "EMPLOYEES", Logger.getAnonymousLogger());
        readerField = Whitebox.getField(SnowflakeGetController.class, "_reader");
        readerField.setAccessible(true);
        setInputs();
    }

    /**
     * Tests the execution of a query with dynamic properties in SnowflakeGetController.
     * Verifies that the reader field value is not null and the next value for SnowflakeTableStream is True.
     */
    @Test
    public void testSnowflakeGetControllerWithDynamicPropertyMap() throws Exception {

        SortedMap<String, String> filterObj = Collections.emptySortedMap();
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.DATABASE, "TEST_SF_DB");
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.SCHEMA, "DEV");

        SimpleTrackedData inputDoc = new SimpleTrackedData(1, inputs.get(0), null, null, dynamicOpProps);
        SnowflakeGetController snowflakeGetController = new SnowflakeGetController(properties, filterObj, inputDoc);

        Assert.assertNotNull(readerField);

        SnowflakeTableStream snowflakeTableStream = (SnowflakeTableStream) readerField.get(snowflakeGetController);
        Assert.assertTrue(snowflakeTableStream.next());
        Assert.assertNotNull(snowflakeGetController.getNext());
    }

    /**
     * Tests that an exception is thrown when SnowflakeGetController is initialized with empty dynamic operation properties.
     * Verifies that the exception message indicates a database access error.
     */
    @Test
    public void testSnowflakeGetControllerWithDynamicPropAsEmpty() {

        thrown.expect(ConnectorException.class);
        thrown.expectMessage("Database access error");

        SortedMap<String, String> filterObj = Collections.emptySortedMap();
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.DATABASE, "");
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.SCHEMA, "");

        SimpleTrackedData inputDoc = new SimpleTrackedData(1, inputs.get(0), null, null, dynamicOpProps);
        new SnowflakeGetController(properties, filterObj, inputDoc);
    }

    /**
     * Tests that an exception is thrown when SnowflakeGetController is initialized with empty schema value.
     * Verifies that the exception message indicates a database access error.
     */
    @Test
    public void testSnowflakeGetControllerWithSchemaAsEmpty() {

        thrown.expect(ConnectorException.class);
        thrown.expectMessage("Database access error");

        SortedMap<String, String> filterObj = Collections.emptySortedMap();
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.DATABASE, "TEST_SF_DB");
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.SCHEMA, "");

        SimpleTrackedData inputDoc = new SimpleTrackedData(1, inputs.get(0), null, null, dynamicOpProps);
        new SnowflakeGetController(properties, filterObj, inputDoc);
    }

    /**
     * Tests the execution of a query with null dynamic property map in SnowflakeGetController.
     * Verifies that the reader field value is not null and the next value for SnowflakeTableStream is True.
     */
    @Test
    public void testSnowflakeGetControllerWithDOPAsNull() throws Exception {
        SortedMap<String, String> filterObj = Collections.emptySortedMap();
        SimpleTrackedData inputDoc = new SimpleTrackedData(1, inputs.get(0), null, null, null);
        SnowflakeGetController snowflakeGetController = new SnowflakeGetController(properties, filterObj, inputDoc);
        Assert.assertNotNull(readerField);

        SnowflakeTableStream snowflakeTableStream = (SnowflakeTableStream) readerField.get(snowflakeGetController);
        Assert.assertTrue(snowflakeTableStream.next());
        Assert.assertNotNull(snowflakeGetController.getNext());
    }

    /**
     * Tests that an exception is thrown when SnowflakeGetController is initialized with null inputDocument.
     * Verifies that a null pointer exception is thrown.
     */
    @Test
    public void testSnowflakeGetControllerWithInputDocAsNull() {

        thrown.expect(NullPointerException.class);
        SortedMap<String, String> filterObj = Collections.emptySortedMap();
        new SnowflakeGetController(properties, filterObj, null);
    }

    private void setInputs() {
        inputs = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            inputs.add(StreamUtil.EMPTY_STREAM);
        }
    }
}

class TestConfig {

    private static final String URL = "jdbc:h2:mem:TEST_SF_DB";
    private static final String USER = "sa";
    private static final String PASSWORD = "pwd";

    /**
     * Creates a database schema and table, then inserts sample data.
     * Initializes an in-memory database with specified schema, table, fields, and SQL statements.
     */
    public static void createDbAndSchema(String schema, String table) {
        List<String> fields = Arrays.asList("id INT AUTO_INCREMENT PRIMARY KEY", "name VARCHAR(255)",
                "salary DECIMAL(10, 2)");
        String insertDataSQL = getInsertStatement(schema, table);

        String querySQL = "SELECT * FROM DEV.employees";
        InMemoryDataBase inMemoryDataBase = new InMemoryDataBase(URL, USER, PASSWORD, schema, table, fields,
                insertDataSQL, querySQL);
        inMemoryDataBase.createDBandSCHEMA();
    }
    /**
     * Generates an SQL insert statement based on the provided schema and table name.
     * Inserts different sets of data depending on whether the schema is "DEV" or not.
     */
    private static String getInsertStatement(String schema, String table) {
        String insertDataSQL;
        if (schema.equals("DEV")) {
            insertDataSQL = "INSERT INTO " + table + " (name, salary) VALUES " + "('John Doe', 50000.00),"
                    + "('Jane Smith', 60000.00)," + "('Alice Johnson', 75000.00)";
        } else {
            insertDataSQL = "INSERT INTO " + table + " (name, salary) VALUES " + "('James Cameron', 50000.00),"
                    + "('Steven Spielberg', 60000.00)," + "('Martin Scorsese', 75000.00)";
        }
        return insertDataSQL;
    }
}
