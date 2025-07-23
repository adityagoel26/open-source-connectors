// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DATA_TYPE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DECIMAL_DIGITS;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TYPE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataExtractorTest {

    private static final String SCHEMA_NAME = "EVENT";
    private static final String OBJECT_TYPE_ID = "Object Type ID";
    private static final String ORACLE_DATABASE_NAME = "Oracle";
    private static final String COLUMN_NAME = "COLUMN NAME";
    private static final String OBJECT_TYPE_ID_COLUMN_NAME = "Object Type ID.COLUMN NAME";

    private final Connection _connection = mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = mock(DatabaseMetaData.class);
    private final ResultSet _resultSet = mock(ResultSet.class);

    private final Map<String, String> expectedDataType = new HashMap<>();

    @Before
    public void setup() throws SQLException {

        when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        when(_databaseMetaData.getDatabaseProductName()).thenReturn(ORACLE_DATABASE_NAME);
    }

    private void setupDataForGetDataTypesWithTable() throws SQLException {

        when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
    }

    private void setupDataForGetDataTypes() throws SQLException {

        when(_databaseMetaData.getColumns(null, SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
    }

    private void setupForResultSet(String databaseConnectorConstants) throws SQLException {

        when(_resultSet.getString(DATA_TYPE)).thenReturn(databaseConnectorConstants);
        setupForResultSetTypeIsJsonAndVarchar("1");
    }

    private void setupForResultSetForRowID(String databaseConnectorConstants) throws SQLException {
        when(_resultSet.getString(DATA_TYPE)).thenReturn(databaseConnectorConstants);
        when(_resultSet.getString(TYPE_NAME)).thenReturn("ROWID");
        when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);
    }

    private void setupForResultSetTypeIsJsonAndVarchar(String json) throws SQLException {

        when(_resultSet.getString(TYPE_NAME)).thenReturn(json);
        when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);
    }

    @Test
    public void testGetDataTypeWithTableTypeNameEqualsJson() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, "JSON");
        setupDataForGetDataTypesWithTable();
        setupForResultSetTypeIsJsonAndVarchar(DatabaseConnectorConstants.JSON);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsVarchar() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.NVARCHAR);
        setupDataForGetDataTypesWithTable();
        setupForResultSetTypeIsJsonAndVarchar(DatabaseConnectorConstants.NVARCHAR);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsLongNVarchar() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.STRING);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.LONGNVARCHAR);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsInteger() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.INTEGER);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.SMALLINT);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsRowId() throws SQLException {
        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.STRING);
        setupDataForGetDataTypesWithTable();
        setupForResultSetForRowID(String.valueOf(Types.ROWID));
        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();
        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsDate() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, "date");
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.DATE);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsBoolean() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.BOOLEAN);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.BOOLEAN);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsTime() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, "time");
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.TIME);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsBigInt() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, "long");
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.BIGINT);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsDouble() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.DOUBLE);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.DOUBLE);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsFloat() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.FLOAT);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.FLOAT);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsVarbinary() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, "BLOB");
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.VARBINARY);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesEqualsTimestamp() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.TIMESTAMP);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.TIMESTAMP);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypesIsDecimalZero() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.INTEGER);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.NUMERIC);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWithTableTypeNumericNonZero() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, DatabaseConnectorConstants.DOUBLE);
        setupDataForGetDataTypesWithTable();
        setupForResultSet(MetadataExtractor.NUMERIC);

        when(_resultSet.getInt(DECIMAL_DIGITS)).thenReturn(1);

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypesWithTableIsNull() throws SQLException {

        expectedDataType.put(OBJECT_TYPE_ID_COLUMN_NAME, "null");
        setupDataForGetDataTypesWithTable();
        setupForResultSet("0");

        Map<String, String> actualDataType = MetadataExtractor.getDataTypesWithTable(_connection, OBJECT_TYPE_ID);
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypeNameIsJson() throws SQLException {

        expectedDataType.put(COLUMN_NAME, "JSON");
        setupDataForGetDataTypes();

        setupForResultSetTypeIsJsonAndVarchar(DatabaseConnectorConstants.JSON);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeTypesEqualsVarchar() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.NVARCHAR);
        setupDataForGetDataTypes();

        setupForResultSetTypeIsJsonAndVarchar(DatabaseConnectorConstants.NVARCHAR);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsLongNVarchar() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.STRING);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.LONGNVARCHAR);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypeIsRowId() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.STRING);
        setupDataForGetDataTypes();
        setupForResultSetForRowID(String.valueOf(Types.ROWID));
        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();
        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsInteger() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.INTEGER);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.SMALLINT);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsDate() throws SQLException {

        expectedDataType.put(COLUMN_NAME, "date");
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.DATE);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsBoolean() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.BOOLEAN);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.BOOLEAN);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsTime() throws SQLException {

        expectedDataType.put(COLUMN_NAME, "time");
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.TIME);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsBigInt() throws SQLException {

        expectedDataType.put(COLUMN_NAME, "long");
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.BIGINT);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsDouble() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.DOUBLE);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.DOUBLE);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsFloat() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.FLOAT);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.FLOAT);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsVarbinary() throws SQLException {

        expectedDataType.put(COLUMN_NAME, "BLOB");
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.VARBINARY);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesIsTimestamp() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.TIMESTAMP);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.TIMESTAMP);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeWhenTypesDecimalIsZero() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.INTEGER);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.NUMERIC);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypeTypesIsDecimalNonZero() throws SQLException {

        expectedDataType.put(COLUMN_NAME, DatabaseConnectorConstants.DOUBLE);
        setupDataForGetDataTypes();
        setupForResultSet(MetadataExtractor.NUMERIC);

        when(_resultSet.getInt(DECIMAL_DIGITS)).thenReturn(1);

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testGetDataTypesIsNull() throws SQLException {

        expectedDataType.put(COLUMN_NAME, "null");
        setupDataForGetDataTypes();
        setupForResultSet("0");

        Map<String, String> actualDataType = new MetadataExtractor(_connection, OBJECT_TYPE_ID,
                SCHEMA_NAME).getDataType();
        String actualValue = String.valueOf(actualDataType.values()).trim();
        String expectedValue = String.valueOf(expectedDataType.values()).trim();

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testisDataTypeIsOfTypeStringTrue() throws SQLException, NoSuchMethodException, InvocationTargetException
            , IllegalAccessException {
        setupForResultSetForRowID(String.valueOf(Types.ROWID));
        Method method = MetadataExtractor.class.getDeclaredMethod("isStringDataType", ResultSet.class);
        method.setAccessible(true);
        boolean actualValue = (boolean) method.invoke(null, _resultSet);
        assertTrue(actualValue);
    }

    @Test
    public void testIsDataTypeIsOfTypeStringFalse() throws SQLException, NoSuchMethodException, InvocationTargetException
            , IllegalAccessException {
        setupForResultSetForRowID(String.valueOf(Types.INTEGER));
        Method method = MetadataExtractor.class.getDeclaredMethod("isStringDataType", ResultSet.class);
        method.setAccessible(true);
        boolean actualValue = (boolean) method.invoke(null, _resultSet);
        assertFalse(actualValue);
    }

    @Test
    public void testIsDataTypeIsOfTypeStringResultSetNull() throws NoSuchMethodException, InvocationTargetException
            , IllegalAccessException {
        Method method = MetadataExtractor.class.getDeclaredMethod("isStringDataType", ResultSet.class);
        method.setAccessible(true);
        boolean actualValue = (boolean) method.invoke(null,(Object)null);
        assertFalse(actualValue);
    }
}