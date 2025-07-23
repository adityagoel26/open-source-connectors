// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.util.IOUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CustomPayloadUtilTest {

    private static final long BATCH_COUNT = 0L;
    private static final int TWO = 2;
    private static final int THREE = 3;
    private static final int FOUR = 4;
    private static final int FIVE = 5;
    private static final int MINUS_FIVE = -5;
    private static final int SIX = 6;
    private static final int MINUS_SIX = -6;
    private static final int SEVEN = 7;
    private static final int EIGHT = 8;
    private static final int MINUS_EIGHT = -8;
    private static final String RESULT_COLUMN = "Affinity";
    private static final String EXPECTED_RESULT_WHEN_NULL = "{\"Affinity\":null}";
    private static final String EXPECTED_RESULT_WHEN_NOTNULL = "{\"Affinity\":5}";
    private static final String EXPECTED_RESULT_WHEN_BIGDECIMAL_NOTNULL_FOR_NUMERIC = "{\"Affinity\":99999999999999999999999999999999999999}";
    private static final String EXPECTED_RESULT_WHEN_BIGDECIMAL_NOTNULL_FOR_DECIMAL = "{\"Affinity\":0.99999999999999999999999999999999999999}";
    private static final String EXPECTED_RESULT_WHEN_DECIMAL_NOTNULL = "{\"Affinity\":5.5}";
    private static final String EXPECTED_RESULT_WHEN_DECIMAL_FALSE = "{\"Affinity\":false}";
    private static final String EXPECTED_RESULT_WHEN_DECIMAL_NULL = "{\"Affinity\":null}";
    private static final String EXPECTED_RESULT_WHEN_ROW_ID_NOTNULL = "{\"Affinity\":\"CCCCODADcAAAAOfACa\"}";
    private static final String EXPECTED_RESULT_NOTNULL = "{}";
    private static final String EXPECTED_RESULT_WHEN_BOOLEAN_FALSE = "{\"Affinity\":false}";
    private static final String EXPECTED_RESULT_WHEN_BOOLEAN_NULL = "{\"Affinity\":null}";
    private static final String EXPECTED_RESULT_WHEN_BOOLEAN_TRUE = "{\"Affinity\":true}";
    private OutputStream _outputStream;
    private CustomPayloadUtil _customPayloadUtil;
    private ResultSet _resultSet;
    private ResultSetMetaData _resultSetMetaData;

    @Before
    public void setup() throws SQLException {
        _resultSet = mock(ResultSet.class);
        _resultSetMetaData = mock(ResultSetMetaData.class);
        _outputStream = new ByteArrayOutputStream();
        _customPayloadUtil = new CustomPayloadUtil(_resultSet, BATCH_COUNT);
        when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        when(_resultSetMetaData.getColumnCount()).thenReturn(1);
        when(_resultSetMetaData.getColumnLabel(anyInt())).thenReturn(RESULT_COLUMN);
        when(_resultSetMetaData.getColumnTypeName(anyInt())).thenReturn("NOTJSON"); //To avoid null pointer exception
    }

    @After
    public void cleanUp() {
        IOUtil.closeQuietly(_outputStream);
    }

    private void doTestSetupInt(int columnType, boolean wasNull, int returnValue) throws SQLException {
        when(_resultSetMetaData.getColumnType(anyInt())).thenReturn(columnType);
        when(_resultSet.wasNull()).thenReturn(wasNull);
        when(_resultSet.getInt(anyString())).thenReturn(returnValue);
    }

    private void doTestSetupLong(int columnType, boolean wasNull, long returnValue) throws SQLException {
        when(_resultSetMetaData.getColumnType(anyInt())).thenReturn(columnType);
        when(_resultSet.wasNull()).thenReturn(wasNull);
        when(_resultSet.getLong(anyString())).thenReturn(returnValue);
    }

    private void doTestSetupDouble(int columnType, boolean wasNull, double returnValue) throws SQLException {
        when(_resultSetMetaData.getColumnType(anyInt())).thenReturn(columnType);
        when(_resultSet.wasNull()).thenReturn(wasNull);
        when(_resultSet.getDouble(anyString())).thenReturn(returnValue);
        when(_resultSetMetaData.getScale(anyInt())).thenReturn(1);
    }

    private void doTestSetupBigDecimal(int columnType, boolean wasNull, BigDecimal returnValue) throws SQLException {
        when(_resultSetMetaData.getColumnType(anyInt())).thenReturn(columnType);
        when(_resultSet.wasNull()).thenReturn(wasNull);
        when(_resultSet.getBigDecimal(anyString())).thenReturn(returnValue);
        when(_resultSetMetaData.getScale(anyInt())).thenReturn(1);
    }

    private void doTestSetupFloat(int columnType, boolean wasNull, float returnValue) throws SQLException {
        when(_resultSetMetaData.getColumnType(anyInt())).thenReturn(columnType);
        when(_resultSet.wasNull()).thenReturn(wasNull);
        when(_resultSet.getFloat(anyString())).thenReturn(returnValue);
    }

    private void doTestSetupString(boolean wasNull, String returnValue) throws SQLException {
        when(_resultSetMetaData.getColumnType(anyInt())).thenReturn(MINUS_EIGHT);
        when(_resultSet.wasNull()).thenReturn(wasNull);
        when(_resultSet.getString(anyString())).thenReturn(returnValue);
    }

    private void doTestSetupBoolean(int columnType, boolean wasNull, boolean returnValue) throws SQLException {
        when(_resultSetMetaData.getColumnType(anyInt())).thenReturn(columnType);
        when(_resultSet.wasNull()).thenReturn(wasNull);
        when(_resultSet.getBoolean(anyString())).thenReturn(returnValue);
    }

    @Test
    public void nonBatchedPayloadColumnTypeFourWithNullTest() throws IOException, SQLException {
        doTestSetupInt(FOUR, true, 0);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeFourWithValueTest() throws IOException, SQLException {
        doTestSetupInt(FOUR, false, 5);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeFiveWithNullTest() throws IOException, SQLException {
        doTestSetupInt(FIVE, true, 0);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();
        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeFiveWithValueTest() throws IOException, SQLException {
        doTestSetupInt(FIVE, false, 5);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeMinusSixWithNullTest() throws IOException, SQLException {
        doTestSetupInt(MINUS_SIX, true, 0);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeMinusSixWithValueTest() throws IOException, SQLException {
        doTestSetupInt(MINUS_SIX, false, 5);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeThreeWithNullTest() throws IOException, SQLException {
        doTestSetupDouble(THREE, true, 0.0);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeThreeWithValueTest() throws IOException, SQLException {
        doTestSetupDouble(THREE, false, 5.5);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_DECIMAL_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeEightWithNullTest() throws IOException, SQLException {
        doTestSetupDouble(EIGHT, true, 0.0);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeEightWithValueTest() throws IOException, SQLException {
        doTestSetupDouble(EIGHT, false, 5.5);
        when(_resultSet.getMetaData().getColumnType(anyInt())).thenReturn(12);
        _customPayloadUtil.writeTo(_outputStream);

        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_DECIMAL_NULL, generatedResult);
    }

    @Test
    public void testBatchedPayloadColumnTypeEightWithValueTestWithCond() throws IOException, SQLException {
        doTestSetupDouble(EIGHT, false, 5.5);
        when(_resultSet.getMetaData().getColumnType(anyInt())).thenReturn(16);
        _customPayloadUtil.writeTo(_outputStream);

        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_DECIMAL_FALSE, generatedResult);
    }

    @Test
    public void testBatchedPayloadColumnWithNotNull() throws IOException, SQLException {
        doTestSetupDouble(EIGHT, false, 5.5);
        when(_resultSet.getMetaData().getColumnType(anyInt())).thenReturn(2004);
        _customPayloadUtil.writeTo(_outputStream);

        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_NOTNULL, generatedResult);
    }

    @Test
    public void testBatchedPayloadColumnTypeEightWithValueWithElseCond() throws IOException, SQLException {
        doTestSetupDouble(EIGHT, false, 5.5);
        when(_resultSetMetaData.getScale(anyInt())).thenReturn(8);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_DECIMAL_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeSixWithNullTest() throws IOException, SQLException {
        doTestSetupFloat(SIX, true, 0.0f);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeSixWithValueTest() throws IOException, SQLException {
        doTestSetupFloat(SIX, false, 5.5f);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_DECIMAL_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeSevenWithNullTest() throws IOException, SQLException {
        doTestSetupFloat(SEVEN, true, 0.0f);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeSevenWithValueTest() throws IOException, SQLException {
        doTestSetupFloat(SEVEN, false, 5.5f);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_DECIMAL_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeMinusFiveWithNullTest() throws IOException, SQLException {
        doTestSetupLong(MINUS_FIVE, true, 0L);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeMinusFiveWithValueTest() throws IOException, SQLException {
        doTestSetupLong(MINUS_FIVE, false, 5L);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeTwoWithNullTest() throws IOException, SQLException {
        doTestSetupBigDecimal(TWO, true, new BigDecimal(0));
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeTwoWithNumericValueBigDecimalTest() throws IOException, SQLException {
        doTestSetupBigDecimal(TWO, false, new BigDecimal("99999999999999999999999999999999999999"));
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_BIGDECIMAL_NOTNULL_FOR_NUMERIC, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeTwoWithDecimalValueBigDecimalTest() throws IOException, SQLException {
        doTestSetupBigDecimal(TWO, false, new BigDecimal("0.99999999999999999999999999999999999999"));
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_BIGDECIMAL_NOTNULL_FOR_DECIMAL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeMinusEightWithValueAsRowIDTest() throws IOException, SQLException {
        doTestSetupString(false, "CCCCODADcAAAAOfACa");
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();
        assertEquals(EXPECTED_RESULT_WHEN_ROW_ID_NOTNULL, generatedResult);
    }

    @Test
    public void nonBatchedPayloadColumnTypeMinusEightWithNullValueTest() throws IOException, SQLException {
        doTestSetupString(true, null);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();
        assertEquals(EXPECTED_RESULT_WHEN_NULL, generatedResult);
    }

    /**
     * Performs a payload generation test for a column of type boolean with a value of 'false'.
     * This test writes a boolean value of 'false' to the CustomPayloadUtil ,
     * and then compares the generated result to the expected result.
     **/
    @Test
    public void nonBatchedPayloadColumnTypeBooleanWithFalse() throws IOException, SQLException {
        doTestSetupBoolean(16, false, false);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_BOOLEAN_FALSE, generatedResult);
    }

    /**
     * Performs a payload generation test for a column of type Boolean with a null value.
     * This test writes a null value to the CustomPayloadUtil,
     * and then compares the generated result to the expected result.
     **/
    @Test
    public void nonBatchedPayloadColumnTypeBooleanWithNull() throws IOException, SQLException {
        doTestSetupBoolean(16, true, false);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_BOOLEAN_NULL, generatedResult);
    }

    /**
     * Performs a payload generation test for a column of type boolean with a value of 'true'.
     * This test writes a boolean value of 'true' to the CustomPayloadUtil ,
     * and then compares the generated result to the expected result.
     **/
    @Test
    public void nonBatchedPayloadColumnTypeBooleanWithTrue() throws IOException, SQLException {
        doTestSetupBoolean(16, false, true);
        _customPayloadUtil.writeTo(_outputStream);
        String generatedResult = _outputStream.toString();

        assertEquals(EXPECTED_RESULT_WHEN_BOOLEAN_TRUE, generatedResult);
    }
}