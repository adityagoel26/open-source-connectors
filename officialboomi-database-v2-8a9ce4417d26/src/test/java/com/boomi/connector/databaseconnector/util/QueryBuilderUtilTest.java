// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.fasterxml.jackson.databind.JsonNode;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Test class for QueryBuilderUtil.
 */
public class QueryBuilderUtilTest {

    public static final String SCHEMA = "dbv2";
    private static final String TABLE_NAME = "test";
    private static final String ID = "id";
    private static final String VALID_COLUMN = "valid_column";
    private static final String TEST_CATALOG = "test_catalog";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";
    private static final String MYSQL_TEST_TABLE = "`test table`";
    private static final String TABLE_WITH_DOUBLE_QUOTES = "\"test table\"";
    private static final String TEST_COLUMN = "test_column";
    private static final String COLUMN_0 = "column0";
    private static final String COLUMN_1 = "column1";
    private static final String COLUMN_2 = "column2";
    private static final String COLUMN_3 = "column3";
    private static final String RESULT_EMPTY_MESSAGE = "Result should be empty";
    private static final String RESULT_NOT_EMPTY_MESSAGE = "Result should not be empty";
    private static final String RESULT_NOT_NULL_MESSAGE = "Result should not be null";
    private static final String DATABASE_ERROR_MESSAGE = "Database error";
    private static final String SIZE_MISMATCH_MESSAGE = "Size does not match expected number of columns";
    private static final String COLUMN_NOT_FOUND_MESSAGE = "Expected column %s was not found in the result set";
    private static final String COLUMN_NOT_FOUND_IN_SET_MESSAGE = "Expected column %s was not found in the set";
    private static final String TABLE_NAME_WITH_SPACE = "test table";
    private static final String TEST_WITH_BACK_TICKS = "Test with BackTicks";
    private static final String TEST_WITH_DOUBLE_QUOTES = "Test with Double Quotes";
    private static final String EXTRA_ESCAPES_BACKSLASHES = "Sales\\\\\\\\Data";
    private static final String ESCAPE_BACKSLASHES = "Sales\\\\Data";
    private static final String TEST_WITH_EXTRA_ESCAPE_BACKSLASHES = "Test with extra Escape backslashes";
    private final JsonNode _node = Mockito.mock(JsonNode.class);
    private final ObjectData objectData = Mockito.mock(ObjectData.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _columnResult = Mockito.mock(ResultSet.class);
    private OperationContext _context = Mockito.mock(OperationContext.class);
    private Set<String> columnNames;

    /**
     * Sets up the mocks for the test cases.
     *
     * @throws SQLException if an SQL exception occurs while setting up the mocks
     */
    @Before
    public void setup() throws SQLException {
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(null, SCHEMA, TABLE_NAME, null)).thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getColumns(null, SCHEMA, MYSQL_TEST_TABLE, null)).thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getColumns(null, null, TABLE_NAME, null)).thenReturn(_columnResult);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_columnResult.next()).thenReturn(true).thenReturn(false);
        columnNames = new LinkedHashSet<>();
    }

    @Test
    public void testStringUnescapingWithSpecialCharacters() {
        Mockito.when(_node.asText()).thenReturn("The person said \\\"Hi, you must be a \\\\\\\"BIG\\\\\\\" football fan!\\\"");
        String expectedOutput = "The person said \"Hi, you must be a \\\"BIG\\\" football fan!\"";
        String actualOutput = QueryBuilderUtil.unescapeEscapedStringFrom(_node);

        Assert.assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testStringUnescapingWithNullValue() {
        Mockito.when(_node.asText()).thenReturn("{ \"Name\": \"John Doe\", \"Address\": " + null + "}");
        String actualOutput = QueryBuilderUtil.unescapeEscapedStringFrom(_node);

        Assert.assertEquals("{ \"Name\": \"John Doe\", \"Address\": " + null + "}", actualOutput);
    }

    @Test
    public void testStringUnescapingWithNullNode() {
        Mockito.when(_node.asText()).thenReturn("null");
        String actualOutput = QueryBuilderUtil.unescapeEscapedStringFrom(_node);

        Assert.assertEquals("null", actualOutput);
    }

    @Test
    public void testStringUnescapingWithNoEscapeCharacters() {
        Mockito.when(_node.asText()).thenReturn("No escape characters here");
        String actualOutput = QueryBuilderUtil.unescapeEscapedStringFrom(_node);

        Assert.assertEquals("No escape characters here", actualOutput);
    }

    /**
     * Test {@link QueryBuilderUtil#unescapeEscapedStringFrom(JsonNode)}
     */
    @Test
    public void testStringUnescapingWithNull() {
        String actualOutput = QueryBuilderUtil.unescapeEscapedStringFrom(null);
        Assert.assertNull(actualOutput);
    }

    @Test
    public void testJsonUnescapingWithSpecialCharacter(){
        Mockito.when(_node.asText()).thenReturn("{\\\"name\\\":\\\"Ron\\\",\\\"age\\\":30,\\\"runs\\\":100}");
        String expectedOutput = "{\"name\":\"Ron\",\"age\":30,\"runs\":100}";
        String actualOutput = QueryBuilderUtil.unescapeEscapedStringFrom(_node);
        Assert.assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testJsonUnescapingWithNullValues(){
        Mockito.when(_node.asText()).thenReturn("{\"Name\":\"Ron\",\"age\":null\"runs\":100}");
        String actualOutput = QueryBuilderUtil.unescapeEscapedStringFrom(_node);
        Assert.assertEquals("{\"Name\":\"Ron\",\"age\":null\"runs\":100}", actualOutput);
    }

    @Test
    public void testGetMaxRowIfConfigureAsDOP(){
        MutableDynamicPropertyMap mutableDynamicPropertyMap = new MutableDynamicPropertyMap();
        Long expectedValue= Long.valueOf(5);
        mutableDynamicPropertyMap.addProperty(DatabaseConnectorConstants.MAX_ROWS,expectedValue);
        Mockito.when(objectData.getDynamicOperationProperties()).thenReturn(mutableDynamicPropertyMap);
        Long actualOutput = QueryBuilderUtil.getMaxRowIfConfigureAsDOP(objectData,4L);
        Assert.assertEquals(expectedValue,actualOutput);
    }

    @Test
    public void testGetMaxRowIfNotConfigureAsDOP(){
        MutableDynamicPropertyMap mutableDynamicPropertyMap = new MutableDynamicPropertyMap();
        Mockito.when(objectData.getDynamicOperationProperties()).thenReturn(mutableDynamicPropertyMap);
        Long actualOutput = QueryBuilderUtil.getMaxRowIfConfigureAsDOP(objectData,4L);
        Assert.assertEquals(Long.valueOf(4),actualOutput);
    }

    @Test
    public void testGetMaxRowIfConfigureAsDOPWithNonLongValue(){
        MutableDynamicPropertyMap mutableDynamicPropertyMap = new MutableDynamicPropertyMap();
        mutableDynamicPropertyMap.addProperty(DatabaseConnectorConstants.MAX_ROWS,"dummy_test");
        Mockito.when(objectData.getDynamicOperationProperties()).thenReturn(mutableDynamicPropertyMap);
        Mockito.when(objectData.getLogger()).thenReturn(Logger.getLogger(QueryBuilderUtil.class.getName()));
        Long actualOutput = QueryBuilderUtil.getMaxRowIfConfigureAsDOP(objectData,4L);
        Assert.assertEquals(Long.valueOf(4),actualOutput);
    }

    @Test
    public void testGetMaxRowIfConfigureAsDOPWithNull(){
        MutableDynamicPropertyMap mutableDynamicPropertyMap = new MutableDynamicPropertyMap();
        mutableDynamicPropertyMap.addProperty(DatabaseConnectorConstants.MAX_ROWS, DatabaseConnectorConstants.MAX_ROWS);
        Mockito.when(objectData.getDynamicOperationProperties()).thenReturn(mutableDynamicPropertyMap);
        Long actualOutput = QueryBuilderUtil.getMaxRowIfConfigureAsDOP(null,4L);
        Assert.assertEquals(Long.valueOf(4),actualOutput);
    }

    @Test
    public void testQueryForUnspecifiedQuestions() {
        StringBuilder sb = new StringBuilder("call sp(?,?,?,?,?,?,?,?)");
        QueryBuilderUtil.removeQuestionMarks(sb,2);
        Assert.assertEquals("call sp(?,?,?,?,?,?)" , sb.toString());
    }

    @Test
    public void testQueryForOneDefaultParam() {
        StringBuilder sb = new StringBuilder("call sp(?)");
        QueryBuilderUtil.removeQuestionMarks(sb,1);
        Assert.assertEquals("call sp()", sb.toString());
    }

    @Test
    public void testQueryForRemoveZeroQuestionParam() {
        StringBuilder sb = new StringBuilder("call sp(?,?,?,?,?)");
        QueryBuilderUtil.removeQuestionMarks(sb,0);
        Assert.assertEquals("call sp(?,?,?,?,?)", sb.toString());
    }

    @Test
    public void testQueryForZeroParamAndOneRemoveParam() {
        StringBuilder sb = new StringBuilder("call ()");
        QueryBuilderUtil.removeQuestionMarks(sb,1);
        Assert.assertEquals("call ()", sb.toString());
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValue() throws SQLException, IOException {
        testExecuteBigValue(
                "999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValueDecimals() throws SQLException, IOException {
        testExecuteBigValue("12345678901234567890123456789012345.12345678901234567890123456789");
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteIntegerBigValue() throws SQLException, IOException {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        DatabaseMetaData  databaseMetaData  = Mockito.mock(DatabaseMetaData.class);
        Connection        connection        = Mockito.mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        HashMap<String, String> dataTypes = new HashMap<>();
        dataTypes.put("integer", "integer");
        QueryBuilderUtil.checkDataType(dataTypes, "integer",
                "99999999999999999999999999999999999999",
                preparedStatement, 1, connection);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(preparedStatement).setBigDecimal(Mockito.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(
                        "99999999999999999999999999999999999999"),
                actual);
    }

    private static void testExecuteBigValue(String number) throws SQLException, IOException {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        DatabaseMetaData  databaseMetaData  = Mockito.mock(DatabaseMetaData.class);
        Connection        connection        = Mockito.mock(Connection.class);
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
        Mockito.when(databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        HashMap<String, String> dataTypes = new HashMap<>();
        dataTypes.put("double", "double");
        QueryBuilderUtil.checkDataType(dataTypes, "double", number,
                preparedStatement, 1, connection);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(preparedStatement).setBigDecimal(Mockito.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(number),
                actual);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithNullPattern() {
        String result = QueryBuilderUtil.replaceWithSqlWildCards(null);
        Assert.assertNull(result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithEmptyPattern() {
        String pattern = "";
        String result  = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertNull(result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithNoWildcards() {
        String pattern = "someString";
        String expected = "someString";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSingleWildcard() {
        String pattern = "*";
        String expected = "%";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSingleWildcardPercentage() {
        String pattern = "%";
        String expected = "%";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithLeadingWildcard() {
        String pattern = "*someString";
        String expected = "%someString";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithTrailingWildcard() {
        String pattern = "someString*";
        String expected = "someString%";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithWildcardInMiddle() {
        String pattern = "some*String";
        String expected = "some%String";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithMultipleWildcards() {
        String pattern = "some*String*With*Wildcards";
        String expected = "some%String%With%Wildcards";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSpacePattern() {
        String pattern = " ";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertNull( result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithSpecialCharacters() {
        String pattern = "!@#$%^&*()";
        String expected = "!@#$%^&%()";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithNewLineCharacters() {
        String pattern = "abc\n";
        String expected = "abc";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test {@link QueryBuilderUtil#replaceWithSqlWildCards(String)}
     */
    @Test
    public void testReplaceWithSqlWildCards_WithTab() {
        String pattern = "abc\tdef";
        String expected = "abc\tdef";
        String result = QueryBuilderUtil.replaceWithSqlWildCards(pattern);
        Assert.assertEquals(expected, result);
    }

    /**
     * Test if {@link QueryBuilderUtil#setSchemaNameInConnection(Connection, String, String)} gives priority to the
     * schema from operation or not.
     *
     * @throws SQLException
     */
    @Test
    public void testSetSchemaNameInConnection() throws SQLException {
        String expected = "SchemaNameFromOperation";
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);

        QueryBuilderUtil.setSchemaNameInConnection(_connection, expected, "schemaNameFromConnectorConnection");

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(_connection).setSchema(argumentCaptor.capture());
        String actual = argumentCaptor.getValue();
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the table has an auto-increment column.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithAutoIncrementReturnsYes() throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT)).thenReturn(
                DatabaseConnectorConstants.YES);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(ID);

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertEquals(ID, autoIncrementColumn);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the table has an auto-increment column.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithAutoIncrementReturnsTrue() throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT)).thenReturn(
                DatabaseConnectorConstants.TRUE);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(ID);

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertEquals(ID, autoIncrementColumn);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the table has an auto-increment column.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithAutoIncrementReturnsOne() throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT)).thenReturn(
                DatabaseConnectorConstants.ONE);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(ID);

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertEquals(ID, autoIncrementColumn);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the table does not have an auto-increment column.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithoutAutoIncrementReturnsNo() throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT)).thenReturn("NO");

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertNull(autoIncrementColumn);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the table does not have an auto-increment column.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithoutAutoIncrementReturnsFalse() throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT)).thenReturn("false");

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertNull(autoIncrementColumn);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the table does not have an auto-increment column.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithoutAutoIncrementReturnsZero() throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT)).thenReturn("0");

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertNull(autoIncrementColumn);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the table does not have an auto-increment column.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithoutAutoIncrementReturnsEmptyString() throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT)).thenReturn("");

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertNull(autoIncrementColumn);
    }

    /**
     * Tests the {@link QueryBuilderUtil#getAutoIncrementColumn(Connection, String, String)} method
     * when the provided table has no columns.
     *
     * @throws SQLException if an SQL exception occurs
     */
    @Test
    public void testGetAutoIncrementColumnWithTableWithNoColumns() throws SQLException {
        Mockito.when(_resultSet.next()).thenReturn(false);

        String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(_connection, TABLE_NAME, SCHEMA);

        Assert.assertNull(autoIncrementColumn);
    }

    /**
     * Tests doesColumnExistInTable method with a null column name.
     * Expected behavior: Should return false when column name is null.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testDoesColumnExistInTable_NullKey() throws SQLException {
        Assert.assertFalse(QueryBuilderUtil.doesColumnExistInTable( null, columnNames));
    }

    /**
     * Tests doesColumnExistInTable method when the column exists in the provided column set.
     * Expected behavior: Should return true when column name is found in the provided set.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testDoesColumnName_ExistingColumnInSet() throws SQLException {
        // Setup
        columnNames.add(VALID_COLUMN);

        // Test
        Assert.assertTrue(QueryBuilderUtil.doesColumnExistInTable(VALID_COLUMN, columnNames));
    }

    /**
     * Tests doesColumnExistInTable method when the column exists in the database table.
     * Expected behavior: Should return true when column name is found in the database table.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testDoesColumnName_ColumnNotExistsInDB() throws SQLException {
        // Test
        Assert.assertFalse(QueryBuilderUtil.doesColumnExistInTable(VALID_COLUMN, null));
    }

    /**
     * Tests getTableColumnsAsSet with null connection.
     * Expected: Should return empty set when connection is null.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSet_NullConnection() throws SQLException {
        Set<String> result = QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, null, TEST_TABLE);

        Assert.assertTrue(result.isEmpty());
    }

    /**
     * Tests getTableColumnsAsSet with null table name.
     * Expected: Should return empty set when table name is null.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSet_NullTableName() throws SQLException {
        Set<String> result = QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, _connection, null);

        Assert.assertTrue(result.isEmpty());
    }

    /**
     * Tests getTableColumnsAsSet with valid parameters and single column.
     * Expected: Should return set containing the column name.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSet_SingleColumn() throws SQLException {
        // Setting up the ResultSet behavior
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, null)).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true, false);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(TEST_COLUMN);

        // Executing test
        Set<String> result = QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, _connection, TEST_TABLE);

        // Verifying results
        Assert.assertFalse(RESULT_NOT_EMPTY_MESSAGE, result.isEmpty());
        Assert.assertEquals(SIZE_MISMATCH_MESSAGE, 1, result.size());
        Assert.assertTrue(result.contains(TEST_COLUMN));
    }

    /**
     * Tests the retrieval of table columns when the table name contains spaces.
     * Verifies that the getTableColumnsAsSet method correctly handles MySQL table names
     * with spaces and returns the expected column set.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSetWithSpaceInTableName() throws SQLException {
        // Setting up the ResultSet behavior
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, TEST_SCHEMA, TABLE_NAME_WITH_SPACE, null)).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true, false);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(TEST_COLUMN);

        // Executing test
        Set<String> result = QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, _connection, MYSQL_TEST_TABLE);

        // Verifying results
        Assert.assertFalse(RESULT_NOT_EMPTY_MESSAGE, result.isEmpty());
        Assert.assertEquals(SIZE_MISMATCH_MESSAGE, 1, result.size());
        Assert.assertTrue(result.contains(TEST_COLUMN));
    }

    /**
     * Tests the retrieval of table columns when the table name contains spaces.
     * Verifies that the getTableColumnsAsSet method correctly handles PostGreSQL table names
     * with spaces and returns the expected column set.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSetWithSpaceInTableNamePostGreSQL() throws SQLException {
        // Setting up the ResultSet behavior
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, TEST_SCHEMA, TABLE_NAME_WITH_SPACE, null)).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true, false);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(TEST_COLUMN);

        // Executing test
        Set<String> result = QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, _connection,
                TABLE_WITH_DOUBLE_QUOTES);

        // Verifying results
        Assert.assertFalse(RESULT_NOT_EMPTY_MESSAGE, result.isEmpty());
        Assert.assertEquals(SIZE_MISMATCH_MESSAGE, 1, result.size());
        Assert.assertTrue(result.contains(TEST_COLUMN));
    }

    /**
     * Tests getTableColumnsAsSet with multiple columns.
     * Expected: Should return set containing all column names.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSet_MultipleColumns() throws SQLException {
        // Setup ResultSet behavior
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, null))
                .thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true, true, true, false);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME))
                .thenReturn(COLUMN_1, COLUMN_2, COLUMN_3);

        // Execute test
        Set<String> result = QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, _connection, TEST_TABLE);

        // Verify results
        Assert.assertEquals(SIZE_MISMATCH_MESSAGE, 3, result.size());
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_IN_SET_MESSAGE, COLUMN_1), result.contains(COLUMN_1));
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_IN_SET_MESSAGE, COLUMN_2), result.contains(COLUMN_2));
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_IN_SET_MESSAGE, COLUMN_3), result.contains(COLUMN_3));
    }

    /**
     * Tests getTableColumnsAsSet with null column names in ResultSet.
     * Expected: Should skip null column names.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSet_NullColumnNames() throws SQLException {
        // Setup ResultSet behavior
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, null)).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true, true, true, false);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_1, null, COLUMN_3);

        // Execute test
        Set<String> result = QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, _connection, TEST_TABLE);

        // Verify results
        Assert.assertEquals(SIZE_MISMATCH_MESSAGE, 2, result.size());
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_IN_SET_MESSAGE, COLUMN_1), result.contains(COLUMN_1));
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_IN_SET_MESSAGE, COLUMN_3), result.contains(COLUMN_3));
        Assert.assertFalse(result.contains(null));
    }

    /**
     * Tests getTableColumnsAsSet when SQLException occurs.
     * Expected: Should throw SQLException.
     *
     * @throws SQLException if database access error occurs
     */
    @Test
    public void testGetTableColumnsAsSet_SQLExceptionThrown() throws SQLException {
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, null)).thenThrow(
                new SQLException(DATABASE_ERROR_MESSAGE));
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        try {
            QueryBuilderUtil.getTableColumnsAsSet(TEST_CATALOG, TEST_SCHEMA, _connection, TEST_TABLE);
            Assert.fail("Expected SQLException was not thrown");
        } catch (SQLException e) {
            Assert.assertEquals(DATABASE_ERROR_MESSAGE, e.getMessage());
        }
    }

    /**
     * Tests extractColumnSetFromCookie with null input.
     * Expected: Should return empty set when cookie string is null.
     */
    @Test
    public void testExtractColumnSetFromCookie_NullInput() {
        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(null);
        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertTrue(RESULT_EMPTY_MESSAGE, result.isEmpty());
    }

    /**
     * Tests extractColumnSetFromCookie with empty string input.
     * Expected: Should return empty set when cookie string is empty.
     */
    @Test
    public void testExtractColumnSetFromCookie_EmptyString() {
        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie("");
        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertTrue(RESULT_EMPTY_MESSAGE, result.isEmpty());
    }

    /**
     * Tests extractColumnSetFromCookie with whitespace string input.
     * Expected: Should return empty set when cookie string contains only whitespace.
     */
    @Test
    public void testExtractColumnSetFromCookie_WhitespaceString() {
        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie("   ");
        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertTrue(RESULT_EMPTY_MESSAGE, result.isEmpty());
    }

    /**
     * Tests extractColumnSetFromCookie with valid JSON but no column names key.
     * Expected: Should return empty set when JSON doesn't contain column names.
     */
    @Test
    public void testExtractColumnSetFromCookie_ValidJsonNoColumnNames() {
        String cookieString = "{\"someOtherKey\": {\"value\": \"test\"}}";
        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(cookieString);
        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertTrue(RESULT_EMPTY_MESSAGE, result.isEmpty());
    }

    /**
     * Tests extractColumnSetFromCookie with valid JSON and column names.
     * Expected: Should return set containing the column names.
     */
    @Test
    public void testExtractColumnSetFromCookie_ValidJsonWithColumnNames() {
        // Create test JSON with column names
        JSONArray columnNames = new JSONArray();
        columnNames.put(COLUMN_1);
        columnNames.put(COLUMN_2);

        JSONObject cookieJson = new JSONObject();
        cookieJson.put(DatabaseConnectorConstants.COLUMN_NAMES_KEY, columnNames);

        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(cookieJson.toString());

        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertEquals("Result should contain 2 columns", 2, result.size());
        Assert.assertTrue("Result should contain column1", result.contains(COLUMN_1));
        Assert.assertTrue("Result should contain column2", result.contains(COLUMN_2));
    }

    /**
     * Tests extractColumnSetFromCookie with invalid JSON string.
     * Expected: Should return empty set when JSON is invalid.
     */
    @Test
    public void testExtractColumnSetFromCookie_InvalidJson() {
        String invalidJson = "{invalid_json:}";
        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(invalidJson);
        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertTrue(RESULT_EMPTY_MESSAGE, result.isEmpty());
    }

    /**
     * Tests extractColumnSetFromCookie with invalid JSON string.
     * Expected: Should return empty set when JSON is invalid.
     */
    @Test
    public void testExtractColumnSetFromCookie_InvalidJsonFormat() {
        String invalidJson = "{\"columnNames\": \"column1\": \"column2\"}}";
        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(invalidJson);
        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertTrue(RESULT_EMPTY_MESSAGE, result.isEmpty());
    }

    /**
     * Tests extractColumnSetFromCookie with empty column names object.
     * Expected: Should return empty set when column names object is empty.
     */
    @Test
    public void testExtractColumnSetFromCookie_EmptyColumnNames() {
        JSONObject cookieJson = new JSONObject();
        cookieJson.put(DatabaseConnectorConstants.COLUMN_NAMES_KEY, new JSONArray());

        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(cookieJson.toString());

        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertTrue(RESULT_EMPTY_MESSAGE, result.isEmpty());
    }

    /**
     * Tests extractColumnSetFromCookie with special characters in column names.
     * Expected: Should handle special characters correctly.
     */
    @Test
    public void testExtractColumnSetFromCookie_SpecialCharacters() {
        JSONArray columnNames = new JSONArray();
        columnNames.put("column@#$%");
        columnNames.put("column with spaces");

        JSONObject cookieJson = new JSONObject();
        cookieJson.put(DatabaseConnectorConstants.COLUMN_NAMES_KEY, columnNames);

        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(cookieJson.toString());

        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertEquals("Result should contain 2 columns", 2, result.size());
        Assert.assertTrue("Result should contain column with special characters",
                result.contains("column@#$%"));
        Assert.assertTrue("Result should contain column with spaces",
                result.contains("column with spaces"));
    }

    /**
     * Tests extractColumnSetFromCookie with large number of column names.
     * Expected: Should handle large number of columns correctly.
     */
    @Test
    public void testExtractColumnSetFromCookie_LargeNumberOfColumns() {

        JSONArray columnNames = new JSONArray();
        int numberOfColumns = 1000;

        for (int i = 0; i < numberOfColumns; i++) {
            columnNames.put("column" + i);
        }

        JSONObject cookieJson = new JSONObject();
        cookieJson.put(DatabaseConnectorConstants.COLUMN_NAMES_KEY, columnNames);

        Set<String> result = QueryBuilderUtil.extractColumnSetFromCookie(cookieJson.toString());

        Assert.assertNotNull(RESULT_NOT_NULL_MESSAGE, result);
        Assert.assertEquals(SIZE_MISMATCH_MESSAGE, numberOfColumns, result.size());

        // Verify some random columns exist
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_MESSAGE, COLUMN_0), result.contains(COLUMN_0));
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_MESSAGE, "column499"), result.contains("column499"));
        Assert.assertTrue(String.format(COLUMN_NOT_FOUND_MESSAGE, "column999"), result.contains("column999"));
    }

    /**
     * This test case verifies that the retrieveTableColumns method retrieves the correct columns
     * when a valid cookie is present in the context.
     *
     * @throws SQLException
     */
    @Test
    public void testRetrieveTableColumns_WithValidCookie() throws SQLException {
        // setup
        Set<String> expectedColumns = new HashSet<>();
        String jsonString = "{ \"columnNames\": [\"column1\", \"column2\"] }";
        expectedColumns.add(COLUMN_1);
        expectedColumns.add(COLUMN_2);

        Mockito.when(_context.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn(jsonString);

        // Actual call
        Set<String> result = QueryBuilderUtil.retrieveTableColumns(_context, _connection, "test");

        // verify the result
        Assert.assertEquals(expectedColumns, result);
        Mockito.verify(_context).getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
        Mockito.verify(_connection, Mockito.never()).getCatalog();
    }

    /**
     * This test case verifies that the retrieveTableColumns method retrieves the correct columns
     * when an empty cookie is present in the context.
     *
     * @throws SQLException
     */
    @Test
    public void testRetrieveTableColumns_WithEmptyCookie() throws SQLException {
        Set<String> expectedColumns = new HashSet<>();
        expectedColumns.add(COLUMN_1);

        setMockContext();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, null)).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true, false);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_1);

        Set<String> result = QueryBuilderUtil.retrieveTableColumns(_context, _connection, TEST_SCHEMA);

        // verify the result
        Assert.assertEquals(expectedColumns, result);
        Mockito.verify(_context).getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
        Mockito.verify(_connection).getCatalog();
    }

    /**
     * This test case verifies that the retrieveTableColumns method throws a SQLException
     * when an invalid cookie is present in the context.
     *
     * @throws SQLException
     */
    @Test
    public void testRetrieveTableColumns_WhenSQLExceptionOccurs() throws SQLException {
        Mockito.when(_context.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn("");
        Mockito.when(_connection.getCatalog()).thenThrow(new SQLException(DATABASE_ERROR_MESSAGE));

        try {
            QueryBuilderUtil.retrieveTableColumns(_context, _connection, TEST_SCHEMA);
            Assert.fail("Expected SQLException was not thrown");
        } catch (SQLException e) {
            Assert.assertEquals(DATABASE_ERROR_MESSAGE, e.getMessage());
        }
    }

    /**
     * This test case verifies that the retrieveTableColumns method retrieves the correct columns
     * when no cookie is present in the context.
     *
     * @throws SQLException
     */
    @Test
    public void testRetrieveTableColumns_WithNullSchema() throws SQLException {
        // Setup
        setMockContext();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(TEST_CATALOG, null, TEST_TABLE, null)).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true, false);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_1);

        // Actual call
        Set<String> result = QueryBuilderUtil.retrieveTableColumns(_context, _connection, null);

        // verify result
        Assert.assertNotNull(result);
        Assert.assertFalse(result.isEmpty());
    }

    /**
     * Tests table name escaping for MySQL database with special characters.
     * Verifies that table names with spaces are properly wrapped with backticks.
     */
    @Test
    public void testCheckSpecialCharacterInDbMySQL(){
            String actualTableName = QueryBuilderUtil.checkSpecialCharacterInDb(MYSQL_TEST_TABLE, DatabaseConnectorConstants.MYSQL);
        Assert.assertEquals(TEST_WITH_BACK_TICKS, TABLE_NAME_WITH_SPACE, actualTableName);
    }

    /**
     * Tests table name escaping for PostGreSQL database with special characters.
     * Verifies that table names with spaces are properly wrapped with Double Quotes.
     */
    @Test
    public void testCheckSpecialCharacterInDbPostGreSQL(){
        String actualTableName = QueryBuilderUtil.checkSpecialCharacterInDb(TABLE_WITH_DOUBLE_QUOTES, DatabaseConnectorConstants.POSTGRESQL);
        Assert.assertEquals(TEST_WITH_DOUBLE_QUOTES, TABLE_NAME_WITH_SPACE, actualTableName);
    }

    /**
     * Tests table name escaping for Oracle database with special characters.
     * Verifies that table names with spaces are properly wrapped with Double Quotes.
     */
    @Test
    public void testCheckSpecialCharacterInDbOracle(){
        String actualTableName = QueryBuilderUtil.checkSpecialCharacterInDb(TABLE_WITH_DOUBLE_QUOTES, DatabaseConnectorConstants.ORACLE);
        Assert.assertEquals(TEST_WITH_DOUBLE_QUOTES, TABLE_NAME_WITH_SPACE, actualTableName);
    }

    /**
     * Tests sanitization of object ID for Oracle database.
     * Verifies that forward slash characters are properly escaped.
     */
    @Test
    public void testSanitizeObjectIdForOracle(){
        String actualTableName = QueryBuilderUtil.checkSpecialCharacterInDb("CUSTOMER/DETAILS", DatabaseConnectorConstants.ORACLE);
        Assert.assertEquals(TEST_WITH_DOUBLE_QUOTES, "CUSTOMER//DETAILS", actualTableName);
    }

    /**
     * Tests escaping of backslashes in table names for PostGreSQL database.
     * Verifies that backslash characters are properly doubled.
     */
    @Test
    public void testSanitizeObjectIdPostGreSQL(){
        String actualTableName = QueryBuilderUtil.checkSpecialCharacterInDb("Sales\\Data", DatabaseConnectorConstants.POSTGRESQL);
        Assert.assertEquals("Test with Escapes backslashes", ESCAPE_BACKSLASHES, actualTableName);
    }

    /**
     * Tests PostGreSQL table name escaping with additional backslashes.
     * Verifies proper handling of backslash characters in table names.
     */
    @Test
    public void testSanitizeObjectIdPostGreSQLWithEscapingBackSlashes(){
        String actualTableName = QueryBuilderUtil.checkSpecialCharacterInDb(EXTRA_ESCAPES_BACKSLASHES, DatabaseConnectorConstants.POSTGRESQL);
        Assert.assertEquals(TEST_WITH_EXTRA_ESCAPE_BACKSLASHES, EXTRA_ESCAPES_BACKSLASHES, actualTableName);
    }

    /**
     * Tests MySQL table name escaping with additional backslashes.
     * Verifies proper handling of backslash characters in table names.
     */
    @Test
    public void testSanitizeObjectIdMySQLWithEscapingBackSlashes(){
        String actualTableName = QueryBuilderUtil.checkSpecialCharacterInDb(EXTRA_ESCAPES_BACKSLASHES, DatabaseConnectorConstants.MYSQL);
        Assert.assertEquals(TEST_WITH_EXTRA_ESCAPE_BACKSLASHES, EXTRA_ESCAPES_BACKSLASHES, actualTableName);
    }

    /**
     * Set up method for context
     *
     * @throws SQLException
     */
    private void setMockContext() throws SQLException {
        Mockito.when(_context.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn("");
        Mockito.when(_context.getObjectTypeId()).thenReturn(TEST_TABLE);
        Mockito.when(_connection.getCatalog()).thenReturn(TEST_CATALOG);
    }
}
