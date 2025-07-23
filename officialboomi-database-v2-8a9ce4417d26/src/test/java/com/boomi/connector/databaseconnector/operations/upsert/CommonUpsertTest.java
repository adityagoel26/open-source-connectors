// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.upsert;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This Class is used to test {@link CommonUpsert Class}
 */
public class CommonUpsertTest {

    private static final String CATALOG = "catalog";
    private final Connection _connection = Mockito.mock(Connection.class);
    private final ResultSet _resultSetMetaExtractor = Mockito.mock(ResultSet.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private Method method;
    private final Class[] parameterTypes = new Class[3];
    private final Object[] parameters = new Object[3];
    private final Class[] updateParameterTypes = new Class[5];
    private final Object[] updateParameters = new Object[5];

    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();
    private final PayloadMetadata _payloadMetadata = Mockito.mock(PayloadMetadata.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);

    @Before
    public void setup() throws SQLException {
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, "EVENT", CATALOG, null)).thenReturn(_resultSetMetaExtractor);

        parameterTypes[0] = PreparedStatement.class;
        parameterTypes[1] = ObjectData.class;
        parameterTypes[2] = Map.class;

        updateParameterTypes[0] = PreparedStatement.class;
        updateParameterTypes[1] = ObjectData.class;
        updateParameterTypes[2] = Map.class;
        updateParameterTypes[3] = List.class;
        updateParameterTypes[4] = List.class;
    }

    /**
     * Verifies that null values in input JSON are correctly set as NULL parameters
     * in the PreparedStatement during upsert operation.
     *
     * @throws SQLException              if database access fails
     * @throws NoSuchMethodException     if method not found
     * @throws InvocationTargetException if method invocation fails
     * @throws IllegalAccessException    if method access denied
     */
    @Test
    @java.lang.SuppressWarnings("java:S3011")
    public void testNullSetWhenPassingInputAsNull()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Map<String, String> dataTypeMap = DataTypesUtil.getDataTypeMap();

        SimpleTrackedData trackedData = new SimpleTrackedData(13,
                new ByteArrayInputStream(DataTypesUtil.INPUT_JSON_NULL_VALUE.getBytes(StandardCharsets.UTF_8)));

        CommonUpsert commonUpsert = new CommonUpsert(_connection, 1L, CATALOG, "Schema Name", "EVENT", false,
                DataTypesUtil.getLinkedHashSet());

        parameters[0] = _preparedStatement;
        parameters[1] = trackedData;
        parameters[2] = dataTypeMap;

        method = commonUpsert.getClass().getDeclaredMethod("appendInsertParams", parameterTypes);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);

        boolean testResult = DataTypesUtil.verifyTestForNullSetExecute(_preparedStatement);
        Assert.assertTrue(testResult);
    }

    /**
     * Tests the handling of string values containing double quotes in input data is correctly set and passed to the
     * PreparedStatement for Insert
     *
     * @throws SQLException              if there is an error executing the database operation
     * @throws IOException               if there is an error reading the input stream
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the invoked method throws an exception
     * @throws IllegalAccessException    if the method cannot be accessed
     */
    @Test
    public void testInsertStringWithDoubleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Given: Input JSON with escaped double quotes
        String inputJson = "{\"field\":\"BeBold\\\"BeBoomi\\\"\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\"BeBoomi\"");
    }

    /**
     * Tests the insert operation with a string containing backslash character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithBackslash()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\\\BeBoomi\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\\BeBoomi");
    }

    /**
     * Tests the insert operation with a string containing new line character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithNewLine()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\nBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\nBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing tab character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithTab()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        String inputJson = "{\"field\":\"BeBold\\tBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\tBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing backspace character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithBackSpace()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\bBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\bBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing carriage return character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithCarriageReturn()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\rBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\rBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing form feed character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithFormFeed()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\fBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\fBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing single quote character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithSingleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"I\'m testing singleQuote\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "I'm testing singleQuote");
    }

    /**
     * Tests the upsert (insert)  operation with a string containing single quote character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertSingleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"'I'm testing singleQuote'\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "'I'm testing singleQuote'");
    }

    /**
     * Tests the upsert (insert)  operation with a string containing comma character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertComma()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"Test,Comma,\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "Test,Comma,");
    }

    /**
     * Tests the upsert (insert) operation with a string containing mixed escape characters.
     * This test verifies that special characters and escape sequences are properly handled
     * during the database update operation.
     * <p>
     * The test includes the following escape sequences:
     * - \t (tab)
     * - \b (backspace)
     * - \n (newline)
     * - \r (carriage return)
     * - \f (form feed)
     * - \' (single quote)
     * - \" (double quote)
     * - \\ (backslash)
     *
     * @throws SQLException              if a database access error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the invoked method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithMixedEscapeCharacters()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson =
                "{\"field\":\"Hello\\tWorld\\bNew\\nLine\\rCarriage\\fFormFeed\'Quote\\\"DoubleQuote\\\\Backslash\"}";
        CommonUpsert commonUpsert = getCommonInsert(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1,
                "Hello\tWorld\bNew\nLine\rCarriage\fFormFeed'Quote\"DoubleQuote\\Backslash");
    }

    /**
     * Tests the handling of string values containing double quotes in input data is correctly set for update operation
     * and passed to the PreparedStatement
     *
     * @throws SQLException              if there is an error executing the database operation
     * @throws IOException               if there is an error reading the input stream
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the invoked method throws an exception
     * @throws IllegalAccessException    if the method cannot be accessed
     */
    @Test
    public void testUpdateStringWithDoubleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"24\\\"-26\\\"\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "24\"-26\"");
    }

    /**
     * Tests the update operation with a string containing backslash character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithBackslash()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\\\BeBoomi\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\\BeBoomi");
    }

    /**
     * Tests the update operation with a string containing new line character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithNewLine()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\nBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\nBeBoomi");
    }

    /**
     * Tests the update operation with a string containing tab character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithTab()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\tBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\tBeBoomi");
    }

    /**
     * Tests the update operation with a string containing back space.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithBackSpace()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\bBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\bBeBoomi");
    }

    /**
     * Tests the update operation with a string containing a Carriage Return character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithCarriageReturn()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\rBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\rBeBoomi");
    }

    /**
     * Tests the update operation with a string containing a Form Feed character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithFormFeed()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\fBeBoomi\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\fBeBoomi");
    }

    /**
     * Tests the update operation with a string containing a single quote character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithSingleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"I\'m testing singleQuote\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "I'm testing singleQuote");
    }

    /**
     * Tests the update operation with a string containing mixed escape characters.
     * This test verifies that special characters and escape sequences are properly handled
     * during the database update operation.
     * <p>
     * The test includes the following escape sequences:
     * - \t (tab)
     * - \b (backspace)
     * - \n (newline)
     * - \r (carriage return)
     * - \f (form feed)
     * - \' (single quote)
     * - \" (double quote)
     * - \\ (backslash)
     *
     * @throws SQLException              if a database access error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the invoked method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateStringWithMixedEscapeCharacters()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson =
                "{\"field\":\"Hello\\tWorld\\bNew\\nLine\\rCarriage\\fFormFeed\'Quote\\\"DoubleQuote\\\\Backslash\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1,
                "Hello\tWorld\bNew\nLine\rCarriage\fFormFeed'Quote\"DoubleQuote\\Backslash");
    }

    /**
     * Tests the Upsert (update) operation with a string containing single quote character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateSingleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"'I'm testing singleQuote'\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement,  Mockito.times(1)).setString(1, "'I'm testing singleQuote'");
    }

    /**
     * Tests the Upsert (update) operation with a string containing comma character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testUpdateComma()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"Test,Comma,\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        method.setAccessible(true);
        method.invoke(commonUpsert, updateParameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "Test,Comma,");
    }

    /**
     * Tests the  upsert operation with String data type handling.
     * Verifies that the operation successfully processes String data type
     * and returns the expected success status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMsSqlString() throws SQLException, IOException, NoSuchMethodException {
        String inputJson = "{\"field\":\"BeBold\\\"BeBoomi\\\"\"}";
        CommonUpsert commonUpsert = getCommonUpdate(inputJson);
        DataTypesUtil.setupInput(inputJson, _updateRequest, simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_resultSetMetaExtractor.next()).thenReturn(true, false);
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(
                MetadataExtractor.VARCHAR);
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("field");
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG, "EVENT", CATALOG)).thenReturn(_resultSetMetaExtractor);
        Mockito.when(_databaseMetaData.getIndexInfo(CATALOG, "EVENT", CATALOG, true, false)).thenReturn(
                _resultSetMetaExtractor);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(
                DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        int[] result = new int[] { 1, 2 };
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(result);
        commonUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, "EVENT", _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\"BeBoomi\"");
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Creates and returns a CommonUpsert instance configured for upsert - insert operations.
     *
     * @param inputJson The JSON input string to be processed
     * @return CommonUpsert instance configured with the specified parameters
     * @throws NoSuchMethodException if the appendInsertParams method cannot be found
     */
    private CommonUpsert getCommonInsert(String inputJson) throws NoSuchMethodException {
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("field", "string");
        Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("field"));
        SimpleTrackedData trackedData = new SimpleTrackedData(13,
                new ByteArrayInputStream(inputJson.getBytes(StandardCharsets.UTF_8)));
        CommonUpsert commonUpsert = new CommonUpsert(_connection, 1L, CATALOG, "Schema Name", "EVENT", false,
                columnNames);
        parameters[0] = _preparedStatement;
        parameters[1] = trackedData;
        parameters[2] = dataTypeMap;
        method = commonUpsert.getClass().getDeclaredMethod("appendInsertParams", parameterTypes);
        return commonUpsert;
    }

    /**
     * Prepares a CommonUpsert instance for upsert - update operations.
     *
     * @param inputJson The JSON input string to be processed
     * @return CommonUpsert instance configured for update operations
     * @throws NoSuchMethodException if the required method cannot be found
     */
    private CommonUpsert getCommonUpdate(String inputJson) throws NoSuchMethodException {
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("field", "string");
        List<String> primaryKeyConflict = new ArrayList<>();
        primaryKeyConflict.add("primaryKey");
        List<String> uniqueKeyConflict = new ArrayList<>();
        uniqueKeyConflict.add("uniqueKey");
        Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("field"));
        SimpleTrackedData trackedData = new SimpleTrackedData(13,
                new ByteArrayInputStream(inputJson.getBytes(StandardCharsets.UTF_8)));
        CommonUpsert commonUpsert = new CommonUpsert(_connection, 1L, CATALOG, "Commit By Rows", "EVENT", false,
                columnNames);
        updateParameters[0] = _preparedStatement;
        updateParameters[1] = trackedData;
        updateParameters[2] = dataTypeMap;
        updateParameters[3] = primaryKeyConflict;
        updateParameters[4] = uniqueKeyConflict;
        method = commonUpsert.getClass().getDeclaredMethod("appendUpdateParams", updateParameterTypes);
        return commonUpsert;
    }
}
