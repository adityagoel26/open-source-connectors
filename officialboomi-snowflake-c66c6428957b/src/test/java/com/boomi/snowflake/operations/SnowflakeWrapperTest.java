// Copyright (c) 2024 Boomi, LP.

package com.boomi.snowflake.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.override.ConnectionOverrideUtilTest;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.ConnectionTimeFormat;
import com.boomi.snowflake.util.SnowflakeDataTypeConstants;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import net.snowflake.client.jdbc.SnowflakeSQLException;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static com.boomi.snowflake.util.SnowflakeDataTypeConstants.SNOWFLAKE_BOOLEANTYPE;
import static com.boomi.snowflake.util.SnowflakeDataTypeConstants.SNOWFLAKE_DATETYPE;
import static com.boomi.snowflake.util.SnowflakeDataTypeConstants.SNOWFLAKE_DOUBLETYPE;
import static com.boomi.snowflake.util.SnowflakeDataTypeConstants.SNOWFLAKE_NUMBERTYPE;
import static com.boomi.snowflake.util.SnowflakeDataTypeConstants.SNOWFLAKE_TIMESTAMP_TZ;
import static com.boomi.snowflake.util.SnowflakeDataTypeConstants.SNOWFLAKE_TIMETYPE;
import static com.boomi.snowflake.util.SnowflakeDataTypeConstants.SNOWFLAKE_VARCHARTYPE;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class SnowflakeWrapperTest {

    private static final String OP_NAME = "create";

    private SnowflakeWrapper _wrapper;
    private final ConnectionTimeFormat _format = mock(ConnectionTimeFormat.class);
    private final SnowflakeConnection _connection = mock(SnowflakeConnection.class);
    private PreparedStatement _statement;
    private Method _fieldValues;

    private final String _type;
    private final String _value;
    private final Map<String, Integer> _occurrences;
    private ConnectionProperties properties;
    private final DynamicPropertyMap dynamicProperties;
    private  final String select_Query="SELECT * FROM table";
    private Map<String, Set<String>> truncatedMap;

    @Mock
    private ConnectionProperties.ConnectionGetter mockConnectionGetter;

    @Mock
    private Connection mockConnection;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public SnowflakeWrapperTest(String type, String value, Map<String, Integer> occurrences,
            DynamicPropertyMap dynamicPropertyMap) {
        _type = type;
        _value = value;
        _occurrences = occurrences;
        this.dynamicProperties = dynamicPropertyMap;
    }

    @Before
    public void setup() throws NoSuchMethodException {
        when(_connection.getOperationContext()).thenReturn(new SnowflakeContextIT(OperationType.CREATE, OP_NAME));
        when(_format.getDateFormat()).thenReturn("MM/dd/yyyy");
        when(_format.getTimeFormat()).thenReturn("HHmmss.SSS");
        when(_format.getDateTimeFormat()).thenReturn("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        properties = new ConnectionProperties(_connection,
                                                                   new MutablePropertyMap(),
                                                                   "TEST_TABLE",
                                                                   Logger.getAnonymousLogger());
        _wrapper = new SnowflakeWrapper(properties.getConnectionGetter(),
                                        _format,
                                        properties.getLogger(),
                                        properties.getTableName());
        _statement = mock(PreparedStatement.class);
        _fieldValues = SnowflakeWrapper.class.getDeclaredMethod("setValuesForFields",
                                                                String.class,
                                                                String.class,
                                                                PreparedStatement.class,
                                                                int.class,
                                                                String.class);
        _fieldValues.setAccessible(true);
    }

    @Test
    public void testSetValuesForFields() throws InvocationTargetException, IllegalAccessException, SQLException {
        _fieldValues.invoke(_wrapper, _type, _value, _statement, 0, "testKey");

        verify(_statement, times(_occurrences.get(SNOWFLAKE_NUMBERTYPE))).setBigDecimal(anyInt(), any());
        verify(_statement, times(_occurrences.get(SNOWFLAKE_DOUBLETYPE))).setDouble(anyInt(), anyDouble());
        verify(_statement, times(_occurrences.get(SNOWFLAKE_BOOLEANTYPE))).setBoolean(anyInt(), anyBoolean());
        verify(_statement, times(_occurrences.get(SNOWFLAKE_DATETYPE))).setDate(anyInt(), any());
        verify(_statement, times(_occurrences.get(SNOWFLAKE_TIMETYPE))).setTime(anyInt(), any());
        verify(_statement, times(_occurrences.get(SNOWFLAKE_TIMESTAMP_TZ))).setTimestamp(anyInt(), any());
        verify(_statement, times(_occurrences.get(SNOWFLAKE_VARCHARTYPE))).setString(anyInt(), anyString());
    }

    @Parameters(name = "{index}: type - {0}, value - {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { SNOWFLAKE_NUMBERTYPE, "42", getOccurrences(SNOWFLAKE_NUMBERTYPE),ConnectionOverrideUtilTest.createDynamicPropertyMap("new db","new schema") },
                { SNOWFLAKE_DOUBLETYPE, "4.2", getOccurrences(SNOWFLAKE_DOUBLETYPE),ConnectionOverrideUtilTest.createDynamicPropertyMap(
                        StringUtil.EMPTY_STRING,"new schema") },
                { SNOWFLAKE_BOOLEANTYPE, "true", getOccurrences(SNOWFLAKE_BOOLEANTYPE),ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",StringUtil.EMPTY_STRING) },
                { SNOWFLAKE_DATETYPE, "02/25/2023", getOccurrences(SNOWFLAKE_DATETYPE),ConnectionOverrideUtilTest.createDynamicPropertyMap(StringUtil.EMPTY_STRING,StringUtil.EMPTY_STRING) },
                { SNOWFLAKE_TIMETYPE, "203342.369", getOccurrences(SNOWFLAKE_TIMETYPE),ConnectionOverrideUtilTest.createDynamicPropertyMap(null,"new schema") },
                { SNOWFLAKE_TIMESTAMP_TZ, "2023-02-25T20:33:42.369Z", getOccurrences(SNOWFLAKE_TIMESTAMP_TZ),ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",null) },
                { SNOWFLAKE_VARCHARTYPE, "random string", getOccurrences(SNOWFLAKE_VARCHARTYPE),ConnectionOverrideUtilTest.createDynamicPropertyMap(StringUtil.EMPTY_STRING,null) },
                });
    }

    private static Map<String, Integer> getOccurrences(String type) {
        Map<String, Integer> occurrences = CollectionUtil.<String, Integer>mapBuilder()
                                                         .put(SNOWFLAKE_NUMBERTYPE, 0)
                                                         .put(SNOWFLAKE_DOUBLETYPE, 0)
                                                         .put(SNOWFLAKE_BOOLEANTYPE, 0)
                                                         .put(SNOWFLAKE_DATETYPE, 0)
                                                         .put(SNOWFLAKE_TIMETYPE, 0)
                                                         .put(SNOWFLAKE_TIMESTAMP_TZ, 0)
                                                         .put(SNOWFLAKE_VARCHARTYPE, 0)
                                                         .finish();
        switch (type) {
            case (SNOWFLAKE_NUMBERTYPE):
                occurrences.put(SNOWFLAKE_NUMBERTYPE, 1);
                break;
            case (SNOWFLAKE_DOUBLETYPE):
                occurrences.put(SNOWFLAKE_DOUBLETYPE, 1);
                break;
            case (SNOWFLAKE_BOOLEANTYPE):
                occurrences.put(SNOWFLAKE_BOOLEANTYPE, 1);
                break;
            case (SNOWFLAKE_DATETYPE):
                occurrences.put(SNOWFLAKE_DATETYPE, 1);
                break;
            case (SNOWFLAKE_TIMETYPE):
                occurrences.put(SNOWFLAKE_TIMETYPE, 1);
                break;
            case (SNOWFLAKE_TIMESTAMP_TZ):
                occurrences.put(SNOWFLAKE_TIMESTAMP_TZ, 1);
                break;
            default:
                occurrences.put(SNOWFLAKE_VARCHARTYPE, 1);
        }
        return occurrences;
    }

    /**
     * Tests the creation of a PreparedStatement using the SnowflakeWrapper.
     * Verifies that the correct methods are called on the mock objects.
     */
    @Test
    public void testCreatePreparedStatement() throws SQLException {

        MockitoAnnotations.initMocks(this);
        _wrapper = new SnowflakeWrapper(mockConnectionGetter, _format, properties.getLogger(),
                properties.getTableName());

        when(mockConnectionGetter.getConnection(any(), ArgumentMatchers.eq(dynamicProperties)))
                .thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(_statement);

        String sqlCommand = select_Query;
        PreparedStatement preparedStatement = _wrapper.createPreparedStatement(sqlCommand, dynamicProperties);
        Assert.assertNotNull(preparedStatement);
        verify(mockConnectionGetter, times(1)).getConnection(any(),
                ArgumentMatchers.eq(dynamicProperties));
        verify(mockConnection, times(1)).prepareStatement(sqlCommand);
    }

    /**
     * Tests the handling of SnowflakeSQLException when creating a PreparedStatement.
     * Verifies that a ConnectorException is thrown with the correct message and cause.
     */
    @Test
    public void testCreatePreparedStatementWithSqlException() throws SQLException {
        MockitoAnnotations.initMocks(this);
        _wrapper = new SnowflakeWrapper(mockConnectionGetter, _format, properties.getLogger(),
                properties.getTableName());

        when(mockConnectionGetter.getConnection(any(), ArgumentMatchers.eq(dynamicProperties)))
                .thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenThrow(
                new SnowflakeSQLException("Incorrect username or password"));

        try {
            _wrapper.createPreparedStatement(select_Query, dynamicProperties);
            Assert.fail("Expected ConnectorException was not thrown");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Errors occurred while building SQL statement"));
            Assert.assertTrue(e.getCause().getMessage().contains("Incorrect username or password"));
            Assert.assertTrue(e.getCause() instanceof SnowflakeSQLException);
        }
    }

    /**
     * Tests the TruncateIfApplicable method of SnowflakeWrapper with various database and schema combinations.
     * Validates the truncation logic and the number of times executeQuery() is called.
     */
    @Test
    public void testTruncateTableIfNotDone() throws Exception {
        MockitoAnnotations.initMocks(this);
        truncatedMap = new HashMap<>();
        _wrapper = new SnowflakeWrapper(mockConnectionGetter, _format, properties.getLogger(),
                properties.getTableName());
        PreparedStatement mockStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnectionGetter.getConnection(any())).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(any())).thenReturn(mockStatement);
        Whitebox.setInternalState(_wrapper, "_truncatedMap", truncatedMap);

        // Test case 1: Verify truncation for db="dataBase1" and schema="schema1"
        // Expected: schema1 should be added to the truncated list for dataBase1, and executeQuery() should be called
        // once.
        verifyTruncate("dataBase1", "schema1", 1, 1, mockStatement);

        /// Test case 2: Verify truncation for db="dataBase1" and schema="schema2"
        // Expected: schema2 should be added to the truncated list for dataBase1, and executeQuery() should be called
        // once.
        verifyTruncate("dataBase1", "schema2", 2, 2, mockStatement);

        // Test case 3: Verify truncation for db="dataBase1" and schema="schema1" again
        // Expected: schema1 should still be present in the truncated list for dataBase1, with a total size of 2.
        // executeQuery() should be called once more.
        verifyTruncate("dataBase1", "schema1", 2, 2, mockStatement);

        // Test case 4: Verify truncation for a different db="dataBase2" and schema="schema1"
        // Expected: schema1 should be added to the truncated list for dataBase2, and executeQuery() should be called
        // once.
        verifyTruncate("dataBase2", "schema1", 1, 3, mockStatement);
    }

    /**
     * Tests that a ConnectorException is thrown when a SQLException occurs during executeQuery.
     */
    @Test
    public void testTruncateThrowsConnectorExceptionOnSQLException() throws Exception {
        MockitoAnnotations.initMocks(this);
        truncatedMap = new HashMap<>();
        _wrapper = new SnowflakeWrapper(mockConnectionGetter, _format, properties.getLogger(),
                properties.getTableName());
        PreparedStatement mockStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnectionGetter.getConnection(any())).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(any())).thenReturn(mockStatement);
        when(mockStatement.executeQuery()).thenThrow(SQLException.class);
        Whitebox.setInternalState(_wrapper, "_truncatedMap", truncatedMap);
        thrown.expect(ConnectorException.class);
        thrown.expectCause(CoreMatchers.isA(SQLException.class));
        thrown.expectMessage(SnowflakeDataTypeConstants.SQL_EXECUTION_ERROR);
        verifyTruncate("dataBase1", "schema1", 1, 1, mockStatement);
        verify(mockStatement).executeQuery();
    }

    /**
     * Tests that a ConnectorException is thrown with BUILDING_SQL_ERROR when SQLException occurs.
     */
    @Test
    public void testTruncateThrowsConnectorExceptionWithBuildingSqlError() throws Exception {
        MockitoAnnotations.initMocks(this);
        truncatedMap = new HashMap<>();
        _wrapper = new SnowflakeWrapper(mockConnectionGetter, _format, properties.getLogger(),
                properties.getTableName());
        PreparedStatement mockStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnectionGetter.getConnection(any())).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(any())).thenThrow(SQLException.class);
        Whitebox.setInternalState(_wrapper, "_truncatedMap", truncatedMap);
        thrown.expect(ConnectorException.class);
        thrown.expectCause(CoreMatchers.isA(SQLException.class));
        thrown.expectMessage(SnowflakeDataTypeConstants.BUILDING_SQL_ERROR);
        verifyTruncate("dataBase1", "schema1", 1, 1, mockStatement);
        verify(mockStatement).executeQuery();
    }

    /**
     * Verifies the truncation behavior for a given database and schema.
     * Checks that the schema is in the truncated list and validates the number of executeQuery() calls.
     */
    private void verifyTruncate(String databaseName, String schemaName, int expectedSize, int expectedQueryCount,
            PreparedStatement mockStatement) throws Exception {
        DynamicPropertyMap dop = ConnectionOverrideUtilTest.createDynamicPropertyMap(databaseName, schemaName);
        _wrapper.truncateTableIfNotDone(dop);

        Set<String> truncatedSchemas = truncatedMap.get(databaseName);
        Assert.assertTrue(truncatedSchemas.contains(schemaName));
        Assert.assertEquals(expectedSize, truncatedSchemas.size());
        Mockito.verify(mockStatement, times(expectedQueryCount)).executeQuery();
    }
}