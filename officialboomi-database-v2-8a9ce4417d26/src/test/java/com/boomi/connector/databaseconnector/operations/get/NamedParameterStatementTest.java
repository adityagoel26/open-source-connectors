// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.get;

import com.boomi.connector.databaseconnector.operations.get.NamedParameterStatement;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NamedParameterStatementTest {

    private final PreparedStatement preparedStatement = mock(PreparedStatement.class);
    private final Connection connection = mock(Connection.class);

    @Before
    public void setup() throws SQLException {

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

    }

    @Test
    public void testNamedParameterParseWithSpaces() {

        String standardGetQuery = "SELECT CONCAT_WS($ '     ' , FIRST_NAME, LAST_NAME) FROM PIYUSH" +
                " WHERE ID=$ID AND ( " + "LAST_NAME=$LAST_NAME OR FIRST_NAME=$FIRST_NAME $)";
        String results = "SELECT CONCAT_WS($ '     ' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=? AND ( LAST_NAME=? OR FIRST_NAME=? ?";
        Map<String, Object> inputIndexMap = new HashMap<>();
        String parsedQuery = NamedParameterStatement.parse(standardGetQuery, inputIndexMap);

        assertNotNull(parsedQuery);
        assertEquals(results, parsedQuery.trim());
        assertTrue(inputIndexMap.size() > 0);
    }

    @Test
    public void testNamedParameterParseWithoutSpaces() {
        String standardGetQuery = "SELECT CONCAT_WS( '' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=$ID AND ( LAST_NAME=$LAST_NAME OR" + " FIRST_NAME=$FIRST_NAME )";
        String results = "SELECT CONCAT_WS( '' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=? AND ( LAST_NAME=? OR " + "FIRST_NAME=? )";
        Map<String, Object> inputIndexMap = new HashMap<>();
        String parsedQuery = NamedParameterStatement.parse(standardGetQuery, inputIndexMap);

        assertNotNull(parsedQuery);
        assertEquals(results, parsedQuery.trim());
        assertTrue(inputIndexMap.size() > 0);
    }

    @Test
    public void testNamedParameterParseWithoutSpace() {
        String standardGetQuery = "SELECT * FROM PIYUSH " +
                "WHERE ID=$ID AND (LAST_NAME=$LAST_NAME OR FIRST_NAME=$FIRST_NAME);";
        String result = "SELECT * FROM PIYUSH " +
                "WHERE ID=? AND (LAST_NAME=? OR FIRST_NAME=?) ";
        Map<String, Object> inputIndexMap = new HashMap<>();
        String parsedQuery = NamedParameterStatement.parse(standardGetQuery, inputIndexMap);

        assertNotNull(parsedQuery);
        assertEquals(result, parsedQuery);
        assertEquals(3, inputIndexMap.size());
    }

    @Test
    public void testNamedParameterParseWithSpace() {
        String standardGetQuery = "SELECT * FROM PIYUSH " +
                "WHERE ID=$ID AND ( LAST_NAME=$LAST_NAME OR FIRST_NAME=$FIRST_NAME )";
        String result = "SELECT * FROM PIYUSH " +
                "WHERE ID=? AND ( LAST_NAME=? OR FIRST_NAME=? ) ";
        Map<String, Object> inputIndexMap = new HashMap<>();
        String parsedQuery = NamedParameterStatement.parse(standardGetQuery, inputIndexMap);
        assertNotNull(parsedQuery);
        assertEquals(result, parsedQuery);
        assertEquals(3, inputIndexMap.size());
    }

    @Test
    public void testInclauseParse() {
        String expectedValue = "SELECT CONCAT_WS('' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=? in " + "( AND ( LAST_NAME=? OR FIRST_NAME= ? )";
        String query = "SELECT CONCAT_WS('' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=$ID in ( AND ( LAST_NAME=$LAST_NAME OR" + " FIRST_NAME= ($name )";
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("Username", 1);


        String actualValue = NamedParameterStatement.inclauseParse(query, paramMap);
        assertEquals(expectedValue.trim(), actualValue.trim());
    }

    @Test
    public void testInClauseParseWithElseCond() {
        String expectedValue = "SELECT CONCAT_WS('' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=? " + "in ( AND ( LAST_NAME=? OR FIRST_NAME= ? )";
        String query = "SELECT CONCAT_WS('' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=$ID " + "in ( AND ( LAST_NAME=$LAST_NAME OR" + " FIRST_NAME= ($name); )";
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("Username", 1);

        String actualValue = NamedParameterStatement.inclauseParse(query, paramMap);

        assertEquals(expectedValue.trim(), actualValue.trim());
    }

    @Test
    public void testbuildParamNameforInClauseWithElseCond() {
        String expectedValue = "SELECT CONCAT_WS('' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=? in ( AND ( LAST_NAME=? OR FIRST_NAME= ? ";
        String query = "SELECT CONCAT_WS('' , FIRST_NAME, LAST_NAME) FROM PIYUSH " +
                "WHERE ID=$ID in ( AND ( LAST_NAME=$LAST_NAME OR" + " FIRST_NAME= ($name); ";
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("Username", 1);


        String actualValue = NamedParameterStatement.inclauseParse(query, paramMap);

        assertEquals(expectedValue.trim(), actualValue.trim());
    }

    @Test
    public void testFillParams() {
        String expectedValue = "SELECT CONCAT_WS IN(?);";
        String query = "SELECT CONCAT_WS " + "IN($firstName);";
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("Username", 1);

        String actualValue = NamedParameterStatement.inclauseParse(query, paramMap);

        assertEquals(expectedValue.trim(), actualValue.trim());
    }

    @Test
    public void testFillParamsWithElseCond() {
        String expectedValue = "SELECT CONCAT_WS IN(?)";
        String query = "SELECT CONCAT_WS IN($firstName) ";
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("Username", 1);

        String actualValue = NamedParameterStatement.inclauseParse(query, paramMap);

        assertEquals(expectedValue.trim(), actualValue.trim());
    }

    @Test
    public void testFillParamsEndsWithBraceSemicolon() {
        String expectedValue = "SELECT CONCAT_WS IN(?) );";
        String query = "SELECT CONCAT_WS IN($firstName );";
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("Username", 1);

        String actualValue = NamedParameterStatement.inclauseParse(query, paramMap);

        assertEquals(expectedValue.trim(), actualValue.trim());
    }

    /**
     * Test Decimal data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testInClauseWithBigDecimal() throws SQLException {
        String query = "SELECT CONCAT_WS FROM PIYUSH WHERE ID=$ID";
        NamedParameterStatement statement = new NamedParameterStatement(connection, query);

        statement.setBigDecimal("ID",
                new BigDecimal("12345678901234567890123456789012345.12345678901234567890123456789"));

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        verify(preparedStatement).setBigDecimal(anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        assertEquals(new BigDecimal("12345678901234567890123456789012345.12345678901234567890123456789"), actual);
    }

    /**
     * Tests the {@link NamedParameterStatement#setNString(String, String)} method with a valid string value.
     * Verifies that the correct value is set on the prepared statement.
     *
     * @throws SQLException if an SQL error occurs during the test.
     */
    @Test
    public void testNvarchar() throws SQLException {
        // Test setup
        String parameterName = "ID";
        String value = "abcdefghijklmnopqrstuvwxyz";

        //verify the execution
        NStringTest(value, parameterName);
    }

    /**
     * Tests the {@link NamedParameterStatement#setNString(String, String)} method with the maximum allowed string length.
     * Verifies that the correct value is set on the prepared statement.
     *
     * @throws SQLException if an SQL error occurs during the test.
     */
    @Test
    public void testNvarchar_MaxLength() throws SQLException {
        // Test setup
        NamedParameterStatement statement = prepareNamedParameterStatement();
        String parameterName = "ID";
        String value = generateMaxNString();

        //verify the execution
        NStringTest(value, parameterName);
    }

    /**
     * Tests the {@link NamedParameterStatement#setNString(String, String)} method with an empty string value.
     * Verifies that the correct value is set on the prepared statement.
     *
     * @throws SQLException if an SQL error occurs during the test.
     */
    @Test
    public void testNvarchar_EmptyValue() throws SQLException {
        // Test setup
        NamedParameterStatement statement = prepareNamedParameterStatement();
        String parameterName = "ID";
        String value = "";

        //verify the execution
        NStringTest(value, parameterName);
    }

    /**
     * Tests the {@link NamedParameterStatement#setNString(String, String)} method with a null string value.
     * Verifies that null is set on the prepared statement.
     *
     * @throws SQLException if an SQL error occurs during the test.
     */
    @Test
    public void testNvarchar_NullValue() throws SQLException {
        // Test setup
        String parameterName = "ID";
        String value = null;

        //verify the execution
        NStringTest(value, parameterName);
    }

    /**
     * Creates a {@link NamedParameterStatement} instance for test setup.
     *
     * @return a {@link NamedParameterStatement} instance.
     */
    private NamedParameterStatement prepareNamedParameterStatement() throws SQLException {
        String query = "SELECT CONCAT_WS FROM PIYUSH WHERE ID=$ID";
        return new NamedParameterStatement(connection, query);
    }

    /**
     * Generates a string with the maximum allowed length for an NCHAR or NVARCHAR type in SQL Server.
     *
     * @return a string with the maximum allowed length.
     */
    private String generateMaxNString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 4000; i++) {
            builder.append('a');
        }
        return builder.toString();
    }

    /**
     * Sets the given NString value to the specified parameter in a SQL statement and verifies the execution.
     *
     * @param value The NString value to be set.
     * @param PARAM_NAME The name of the parameter to set the NString value to.
     * @throws SQLException If a database access error occurs or the parameter index is out of range.
     */
    private void NStringTest(String value, String PARAM_NAME) throws SQLException {
        NamedParameterStatement statement = prepareNamedParameterStatement();
        // Test execution
        statement.setNString(PARAM_NAME, value);
        // Verification
        verify(preparedStatement).setNString(anyInt(), eq(value));
    }
}



