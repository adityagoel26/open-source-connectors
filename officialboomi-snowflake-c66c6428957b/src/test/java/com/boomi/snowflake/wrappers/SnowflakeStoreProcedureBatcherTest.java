// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.wrappers;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.override.ConnectionOverrideUtilTest;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.ConnectionTimeFormat;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.util.StringUtil;
import net.snowflake.client.jdbc.SnowflakeStatement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Unit test class for the {@link SnowflakeStoredProcedureBatcher} class.
 * Tests various scenarios of adding stored procedure calls with valid and invalid dynamic property maps.
 */
@RunWith(Parameterized.class)
public class SnowflakeStoreProcedureBatcherTest {
    private static final String TABLE_NAME =
            "\"TEST_SF_DB\".\"DEF\".\"FIND_EMPLOYEE_BY_ID\".(ID NUMBER).TABLE (ID NUMBER, NAME VARCHAR, AGE NUMBER)";
    private Long batchSize = 1L;
    private String sqlCommand = "CALL \"FIND_EMPLOYEE_BY_ID\"(1)";
    private SnowflakeStoredProcedureBatcher batcher;
    private List<ObjectData> requestDataArray;
    private ObjectData _objectData;
    private ObjectData _objectData1;
    private final MutableDynamicPropertyMap dynamicOpProps;
    private final MutableDynamicPropertyMap invalidDynamicOpProps;
    private int currentStatement = 0;
    private ConnectionProperties properties;

    @Mock
    private ConnectionProperties.ConnectionGetter _connectionGetter;
    @Mock
    private OperationResponse _operationResponse;
    @Mock
    private Logger _logger;
    @Mock
    private Statement mockStatement;
    @Mock
    private Connection connectionMock;
    @Mock
    private ConnectionTimeFormat _connectionTimeFormat;
    @Mock
    private SnowflakeConnection connection;
    @Mock
    private SnowflakeStatement snowflakeStatement;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Constructor for the test class to initialize parameters.
     *
     * @param dynamicOpProps        Valid dynamic property map for the operation
     * @param invalidDynamicOpProps Invalid dynamic property map for testing
     */
    public SnowflakeStoreProcedureBatcherTest(MutableDynamicPropertyMap dynamicOpProps,
            MutableDynamicPropertyMap invalidDynamicOpProps) {
        super();
        this.dynamicOpProps = dynamicOpProps;
        this.invalidDynamicOpProps = invalidDynamicOpProps;
    }

    /**
     * Provides the test data for parameterized tests.
     *
     * @return A collection of test data arrays
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {
                        ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEV"),
                        ConnectionOverrideUtilTest.createDynamicPropertyMap(StringUtil.EMPTY_STRING,
                                StringUtil.EMPTY_STRING)
                }, {
                ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEF"),
                ConnectionOverrideUtilTest.createDynamicPropertyMap(" ", " ")
        }, {
                ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEV"),
                ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "")
        },
                {
                    ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB", "DEV"),
                        new MutableDynamicPropertyMap()
                }});
    }

    /**
     * Sets up the necessary mocks and objects before each test.
     *
     * @throws SQLException if an error occurs while setting up
     */
    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.initMocks(this);

        // Mocking object data and statement interactions
        requestDataArray = PowerMockito.mock(List.class);
        _objectData = PowerMockito.mock(ObjectData.class);
        _objectData1 = PowerMockito.mock(ObjectData.class);

        // Mocking the connection and properties
        Mockito.when(connection.getOperationContext())
                .thenReturn(new SnowflakeContextIT(OperationType.EXECUTE, "EXECUTE"));
        properties = new ConnectionProperties(connection, new MutablePropertyMap(), "EMPLOYEE",
                Logger.getAnonymousLogger());
        Mockito.when(connection.createJdbcConnection()).thenReturn(connectionMock);
        Mockito.when(connection.createJdbcConnection().createStatement()).thenReturn(mockStatement);
        Mockito.when(connectionMock.getCatalog()).thenReturn("TEST_CATALOG");
        Mockito.when(connectionMock.getSchema()).thenReturn("PUBLIC");
        Mockito.when(mockStatement.getConnection()).thenReturn(connectionMock);
        Mockito.when(connectionMock.getCatalog()).thenAnswer(invocation -> dynamicOpProps.getProperty("db"));
        Mockito.when(connectionMock.getSchema()).thenAnswer(invocation -> dynamicOpProps.getProperty("schema"));
        Mockito.when(_connectionGetter.getStatement(dynamicOpProps)).thenReturn(mockStatement);
        Mockito.when(mockStatement.getConnection()).thenReturn(connectionMock);
        Mockito.when(mockStatement.unwrap(SnowflakeStatement.class)).thenReturn(snowflakeStatement);
        Mockito.doNothing().when(snowflakeStatement).setParameter("MULTI_STATEMENT_COUNT", 1L);
        Mockito.when(_connectionGetter.getStatement(dynamicOpProps).execute(sqlCommand)).thenReturn(true);
        Mockito.when(requestDataArray.get(currentStatement)).thenReturn(_objectData);
        Mockito.when(requestDataArray.get(currentStatement).getLogger()).thenReturn(_logger);

        // Initializing the batcher
        batcher = new SnowflakeStoredProcedureBatcher(properties.getConnectionGetter(), _connectionTimeFormat,
                properties.getLogger(), TABLE_NAME, batchSize);
    }

    /**
     * Test method for adding a call with a valid DynamicPropertyMap.
     *
     * @throws SQLException if an error occurs during the test
     */
    @Test
    public void testAddCallWithValidDynamicPropertyMap() throws SQLException {
        SortedMap<String, String> jsonCall = new TreeMap<>();
        jsonCall.put("ID", "1");
        List<ObjectData> requestDataArray = new ArrayList<>(Arrays.asList(_objectData, _objectData1));
        Mockito.when(_connectionGetter.getStatement(dynamicOpProps).getConnection()).thenReturn(connectionMock);
        batcher.addCall(jsonCall, requestDataArray, _operationResponse, dynamicOpProps);
        Assert.assertEquals(dynamicOpProps.getProperty("db"), connectionMock.getCatalog());
        Assert.assertEquals(dynamicOpProps.getProperty("schema"), connectionMock.getSchema());
    }

    /**
     * Test method for adding a call with an invalid DynamicPropertyMap.
     * Expects a ConnectorException to be thrown.
     *
     * @throws SQLException if an error occurs during the test
     */
    @Test
    public void testAddCallWithInvalidDynamicPropertyMap() throws SQLException {
        SortedMap<String, String> jsonCall = new TreeMap<>();
        jsonCall.put("ID", "1");
        // Simulate a failure scenario with invalid dynamic properties
        Mockito.when(mockStatement.getConnection()).thenThrow(new ConnectorException("Database access error"));
        List<ObjectData> requestDataArray = new ArrayList<>(Arrays.asList(_objectData, _objectData1));
        // Expect an exception to be thrown
        thrown.expect(ConnectorException.class);
        thrown.expectMessage("Database access error");
        batcher.addCall(jsonCall, requestDataArray, _operationResponse, invalidDynamicOpProps);
    }
}
