// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.wrappers;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.override.ConnectionOverrideUtilTest;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.InMemoryDataBase;
import com.boomi.snowflake.util.SnowflakeContextIT;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Logger;

@RunWith(Parameterized.class)
public class SnowflakeTableStreamTest {

    private final List<String> selectedColumns;
    private final SortedMap<String, String> filterObj;
    private final List<String> orderItems;
    private final DynamicPropertyMap invalidDynamicOpProps;
    private final String expected;
    private DynamicPropertyMap dynamicOpProps;
    private Field resultSetField;
    private ConnectionProperties properties;
    private static final SortedMap<String, String> _sortedMap = new TreeMap<>();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public SnowflakeTableStreamTest(List<String> selectedColumns, SortedMap<String, String> filterObj,
                                    List<String> orderItems, MutableDynamicPropertyMap dynamicOpProps,
                                    DynamicPropertyMap invalidDynamicOpProps, String expected) {
        this.selectedColumns = selectedColumns;
        this.filterObj = filterObj;
        this.orderItems = orderItems;
        this.dynamicOpProps = dynamicOpProps;
        this.invalidDynamicOpProps = invalidDynamicOpProps;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {
                        Collections.emptyList(), _sortedMap, Collections.emptyList(),
                        ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEV"),
                        ConnectionOverrideUtilTest.createDynamicPropertyMap("", ""), "John Doe"
                },
                {
                        Collections.emptyList(), Collections.emptySortedMap(), Collections.emptyList(),
                        ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEV"),
                        ConnectionOverrideUtilTest.createDynamicPropertyMap("", ""), "John Doe"}
                , {
                Collections.emptyList(), Collections.emptySortedMap(), Collections.emptyList(),
                ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEF"),
                ConnectionOverrideUtilTest.createDynamicPropertyMap(" ", " "), "James Cameron"}
                , {
                    Collections.emptyList(), Collections.emptySortedMap(), Collections.emptyList(),
                ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEV"),
                ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", ""), "John Doe"}
        });
    }

    @Before
    public void setUp() {
        TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
        TestConfig.createDbAndSchema("DEF", "EMPLOYEES");
        SnowflakeContextIT testContext = new SnowflakeContextIT(OperationType.GET, "get");
        SnowflakeConnection connection = new SnowflakeConnection(testContext);
        properties = new ConnectionProperties(connection, new MutablePropertyMap(), "EMPLOYEES",
                Logger.getAnonymousLogger());
        resultSetField = Whitebox.getField(SnowflakeTableStream.class, "_resultSet");
        resultSetField.setAccessible(true);
        testContext.addConnectionProperty("connectionString","jdbc:h2:mem:TEST_SF_DB");
        testContext.addConnectionProperty("user", System.getProperty("UserName", "sa"));
        testContext.addConnectionProperty("password", System.getProperty("Password", "pwd"));
        testContext.addConnectionProperty("db", "\"TEST_SF_DB\"");
        testContext.addConnectionProperty("schema", "DEV");
        testContext.addConnectionProperty("enablePooling", false);
        _sortedMap.put("NAME", "John Doe");
    }

    /**
     * Tests the execution of a query with dynamic properties in SnowflakeTableStream.
     * Verifies that the result set values match the expected and updated values based on schema changes.
     */
    @Test
    public void testExecuteQueryWithDynamicPropertyMap() throws Exception {
        SnowflakeTableStream snowflakeTableStream = new SnowflakeTableStream(properties, selectedColumns, filterObj,
                orderItems, null, dynamicOpProps);

        Assert.assertNotNull(resultSetField);
        ResultSet stream = (ResultSet) resultSetField.get(snowflakeTableStream);
        Assert.assertTrue(stream.next());
        Assert.assertEquals(expected, stream.getString(2));
        String updateSchema = dynamicOpProps.getProperty("schema").equals("DEV") ? "DEF" : "DEV";
        dynamicOpProps = ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", updateSchema);
        String updatedValue = dynamicOpProps.getProperty("schema").equals("DEV") ? "John Doe" : "James Cameron";
        _sortedMap.put("NAME", updatedValue);
        SnowflakeTableStream updatedsnowflakeTableStream = new SnowflakeTableStream(properties, selectedColumns,
                filterObj, orderItems, null, dynamicOpProps);
        Assert.assertNotNull(resultSetField);
        stream = (ResultSet) resultSetField.get(updatedsnowflakeTableStream);
        Assert.assertTrue(stream.next());
        Assert.assertEquals(updatedValue, stream.getString(2));
    }

    /**
     * Tests that an exception is thrown when SnowflakeTableStream is initialized with invalid database properties.
     * Verifies that the exception message indicates a database access error.
     */
    @Test
    public void testExecuteGETWithEmptyDBandSchemaAsInvalid() {
        thrown.expect(ConnectorException.class);
        thrown.expectMessage("Database access error");
        new SnowflakeTableStream(properties, selectedColumns, filterObj, orderItems, null,
                invalidDynamicOpProps);
    }

    /**
     * Tests the execution of a query without a dynamic property map in SnowflakeTableStream.
     * Verifies that the result set contains the expected value in the second column.
     */
    @Test
    public void testExecuteQueryWithoutDynamicPropertyMap() throws Exception {
        SnowflakeTableStream snowflakeTableStream = new SnowflakeTableStream(properties, selectedColumns, filterObj,
                orderItems, null, new MutableDynamicPropertyMap());
        Assert.assertNotNull(resultSetField);
        ResultSet stream = (ResultSet) resultSetField.get(snowflakeTableStream);
        Assert.assertTrue(stream.next());
        Assert.assertEquals("John Doe", stream.getString(2));
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