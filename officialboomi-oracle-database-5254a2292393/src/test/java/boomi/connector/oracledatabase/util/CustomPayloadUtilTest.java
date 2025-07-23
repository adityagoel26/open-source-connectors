// Copyright (c) 2023 Boomi, Inc.
package boomi.connector.oracledatabase.util;

import com.boomi.connector.oracledatabase.util.CustomPayloadUtil;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CustomPayloadUtilTest {

    private static final String MAX_VALUE_THAN_INT = "21474836473475483574389";
    private static final String COLUMN = "Quantity";
    private final ResultSet _resultSet = mock(ResultSet.class);
    private final Connection _connection = mock(Connection.class);
    private final ResultSetMetaData _resultSetMetaData = mock(ResultSetMetaData.class);
    private final CustomPayloadUtil customPayloadUtil = new CustomPayloadUtil(_resultSet, _connection, 0);
    private OutputStream _outputStream;

    @Before
    public void setUp() throws SQLException {

        when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        when(_resultSetMetaData.getColumnCount()).thenReturn(1);
        when(_resultSetMetaData.getColumnType(1)).thenReturn(2);
        when(_resultSetMetaData.getColumnLabel(anyInt())).thenReturn(COLUMN);

        _outputStream = new ByteArrayOutputStream();
    }

    /**
     * This test is used to check whether more than Int limit fetched without numeric overflow error for NUMBER column
     */
    @Test
    public void columnTypeTwoMoreThanIntLimitTest() throws IOException, SQLException {
        when(_resultSet.getBigDecimal(anyString())).thenReturn(new BigDecimal(MAX_VALUE_THAN_INT));

        customPayloadUtil.writeTo(_outputStream);
        String actual_result = _outputStream.toString();
        String expected_result = "{\"Quantity\":21474836473475483574389}";
        assertEquals(expected_result, actual_result);
    }

    /**
     * This test is used to check whether more than Int limit decimal fetched without numeric overflow error for NUMBER
     * column
     */
    @Test
    public void columnTypeTwoMoreThanIntLimitDecimalTest() throws IOException, SQLException {
        when(_resultSet.getBigDecimal(anyString())).thenReturn(new BigDecimal("9999999999999999999999.9898"));

        customPayloadUtil.writeTo(_outputStream);
        String actual_result = _outputStream.toString();
        String expected_result = "{\"Quantity\":9999999999999999999999.9898}";
        assertEquals(expected_result, actual_result);
    }

    /**
     * This test is used to check whether Integer MAX_VALUE fetched without numeric overflow error for NUMBER column
     */
    @Test
    public void columnTypeTwoMaxIntLimitTest() throws IOException, SQLException {
        when(_resultSet.getBigDecimal(anyString())).thenReturn(new BigDecimal(Integer.MAX_VALUE));

        customPayloadUtil.writeTo(_outputStream);
        String actual_result = _outputStream.toString();
        String expected_result = "{\"Quantity\":2147483647}";
        assertEquals(expected_result, actual_result);
    }

    /**
     * This test is used to check whether Integer MIN_VALUE fetched without numeric overflow error for NUMBER column
     */
    @Test
    public void columnTypeTwoMinIntLimitTest() throws IOException, SQLException {
        when(_resultSet.getBigDecimal(anyString())).thenReturn(new BigDecimal(Integer.MIN_VALUE));

        customPayloadUtil.writeTo(_outputStream);
        String actual_result = _outputStream.toString();
        String expected_result = "{\"Quantity\":-2147483648}";
        assertEquals(expected_result, actual_result);
    }

    /**
     * This test is used to check whether less than Integer MIN_VALUE fetched without numeric overflow error for NUMBER
     * column
     */
    @Test
    public void columnTypeTwoLessThanIntLimitTest() throws IOException, SQLException {
        when(_resultSet.getBigDecimal(anyString())).thenReturn(new BigDecimal("-4732746732648273472"));

        customPayloadUtil.writeTo(_outputStream);
        String actual_result = _outputStream.toString();
        String expected_result = "{\"Quantity\":-4732746732648273472}";
        assertEquals(expected_result, actual_result);
    }

    /**
     * This test is used to check whether within INT limit fetched without numeric overflow error for NUMBER
     * column
     */
    @Test
    public void columnTypeTwoWithinIntLimitTest() throws IOException, SQLException {
        when(_resultSet.getBigDecimal(anyString())).thenReturn(new BigDecimal(20000));

        customPayloadUtil.writeTo(_outputStream);
        String actual_result = _outputStream.toString();
        String expected_result = "{\"Quantity\":20000}";
        assertEquals(expected_result, actual_result);
    }
}