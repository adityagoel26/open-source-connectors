// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.testutil;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;

import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Utility class providing helper methods for setting up test data and mock objects
 * for database operations testing
 */
public class DataTypesUtil {

    private DataTypesUtil() {

    }

    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_NAME_ID = "id";
    private static final String COLUMN_NAME_TIME = "time";
    private static final String COLUMN_NAME_DATE = "date";
    private static final String COLUMN_NAME_BOOLEAN = "isValid";
    private static final String COLUMN_NAME_BLOB = "Shop";
    private static final String COLUMN_NAME_CLOB = "Customer";
    private static final String COLUMN_NAME_LONG = "phNo";
    private static final String COLUMN_NAME_FLOAT = "price";
    public static final String ERROR_MESSAGE = "Response status is not SUCCESS";
    public static final String ERROR_MESSAGE_STATUS_CODE="Response CODE is not SUCCESS";
    public static final String ERROR_MESSAGE_STATUS_MESSAGE="Response MESSAGE is not SUCCESS";
    public static final String APPLICATION_ERROR_MESSAGE = "Response status is not APPLICATION ERROR";
    public static final String FAILURE_ERROR_MESSAGE = "Response status is not FAILURE";
    public static final String CONSTRAINT_EXCEPTION_MESSAGE = "ORA-00001: unique constraint (DEVUSER.PK_EMP_ID) violated";

    public static final String INPUT =
            "{\r\n" + "\"id\":\"123\",\r\n" + " \"double\":\"2023-04-06\",\r\n" + "\"float\":\"123.00\",\r\n" + " \"Long\":\"1234\",\r\n"
                    + "\"date\":\"2023-04-06\",\r\n" + " \"name\":\"abc\",\r\n" + "\"dob\":\"2019-12-09\",\r\n" + "\"clob\":\"Hello\",\r\n"
                    + "\"isqualified\":true,\r\n" + " \"laptime\":\"21:09:08\"\r\n" + "\r\n" + " }";

    public static final String INPUT_JSON_NULL_VALUE =
            "{\"id\":30,\"sno\":null,\"name\":null,\"sdate\":null,\"stime\":null,\"distance\":null,\"salary\":null,"
                    + "\"bonus\":null,\"clob\":null,\"isqualified\":null,\"result\":\"Success\"}";

    public static void setupInput(String input, UpdateRequest _updateRequest, SimpleOperationResponse simpleOperationResponse) {
        setupTrackedData(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), _updateRequest,
                simpleOperationResponse);
    }

    public static void setupTrackedData(InputStream result, UpdateRequest updateRequest,
            SimpleOperationResponse simpleOperationResponse) {

        SimpleTrackedData trackedData = new SimpleTrackedData(13, result);
        List<ObjectData> trackedDataList = new ArrayList<>();
        trackedDataList.add(trackedData);
        Iterator<ObjectData> objDataItr = trackedDataList.listIterator();
        Mockito.when(updateRequest.iterator()).thenReturn(objDataItr);
        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
    }

    /**
     * Mocks update request to return multiple docs.
     * @param result
     * @param updateRequest
     * @param simpleOperationResponse
     */
    public static void setupMultipleTrackedData(List<InputStream> result, UpdateRequest updateRequest,
            SimpleOperationResponse simpleOperationResponse) {
        List<ObjectData> trackedDataList = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {
            SimpleTrackedData trackedData = new SimpleTrackedData(i, result.get(i));
            trackedDataList.add(trackedData);
            SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
        }
        Iterator<ObjectData> objDataItr = trackedDataList.listIterator();
        Mockito.when(updateRequest.iterator()).thenReturn(objDataItr);
    }

    public static void seUpTrackedDataList(UpdateRequest updateRequest, SimpleOperationResponse simpleOperationResponse,
            List<SimpleTrackedData> simpleTrackedDataList) {

        List<ObjectData> trackedDataList = new ArrayList<>(simpleTrackedDataList);
        Iterator<ObjectData> objDataItr = trackedDataList.listIterator();
        Mockito.when(updateRequest.iterator()).thenReturn(objDataItr);
        for (ObjectData trackData : trackedDataList) {
            SimpleOperationResponseWrapper.addTrackedData((SimpleTrackedData) trackData, simpleOperationResponse);
        }
    }

    public static void setUpResultSetBlob(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.VARBINARY);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_BLOB);
    }

    public static void setUpResultSetBoolean(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.BOOLEAN);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_BOOLEAN);
    }

    public static void setUpResultSetTimestamp(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.TIMESTAMP);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_TIME);
    }

    public static void setUpResultSetFloat(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.FLOAT);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_FLOAT);
    }

    public static void setUpResultSetNumericDouble(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.NUMERIC);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getInt(DatabaseConnectorConstants.DECIMAL_DIGITS)).thenReturn(1);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_FLOAT);
    }

    public static void setUpResultSetNumericInteger(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.NUMERIC);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getInt(DatabaseConnectorConstants.DECIMAL_DIGITS)).thenReturn(0);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_FLOAT);
    }

    public static void setUpResultSetLong(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.BIGINT);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_LONG);
    }

    public static void setUpResultSetString(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.VARCHAR);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
    }

    public static void setUpResultSetJson(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.VARCHAR);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
    }

    public static void setUpResultSetForVarChar(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.NVARCHAR);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_ID);
    }

    public static void setUpResultSetForIntegerForException(ResultSet _resultSet) throws SQLException {
        setUpResultSetForInteger(_resultSet);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
    }

    public static void setUpResultSetForInteger(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.INTEGER);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_ID);
    }

    public static void setUpResultSetForTime(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.TIME);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_TIME);
    }

    public static void setUpForDateException(ResultSet _resultSet) throws SQLException {
        setUpResultSetForDate(_resultSet);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_ID);
    }

    public static void setUpResultSetForDate(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.DATE);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("1");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_DATE);
    }

    public static void setUpResultSetClob(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(MetadataExtractor.CLOB);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("CLOB");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_CLOB);
    }

    public static void setupOperationContextObject(OperationContext _operationContext, PropertyMap _propertyMap,
            String OBJECT_TYPE_ID) {
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
    }

    public static void setUpConnectionObject(Connection _connection, DatabaseMetaData _databaseMetaData,
            PreparedStatement _preparedStatement, DatabaseConnectorConnection _databaseConnectorConnection) throws SQLException {
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareStatement(Mockito.anyString(), Mockito.anyInt())).thenReturn(_preparedStatement);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
    }

    /**
     * Setup connection object for transaction.
     * @param _connection
     * @param _databaseMetaData
     * @param _preparedStatement
     * @param _databaseConnectorConnection
     * @throws SQLException
     */
    public static void setUpTransactionConnectionObject(Connection _connection, DatabaseMetaData _databaseMetaData,
            PreparedStatement _preparedStatement, TransactionDatabaseConnectorConnection _databaseConnectorConnection) throws SQLException {
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareStatement(Mockito.anyString(), Mockito.anyInt())).thenReturn(_preparedStatement);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
    }

    public static void setUpResultObjectData(ResultSet _resultSet) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
    }

    public static void setUpObjectForStandardOperation(DatabaseMetaData _databaseMetaData, PreparedStatement _preparedStatement,
            ResultSet _resultSet, PropertyMap _propertyMap, List<ObjectType> _list, String OBJECT_TYPE_ID) throws SQLException {

        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_list.size()).thenReturn(2);
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.GET_TYPE)).thenReturn("get");
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.TYPE)).thenReturn("type");

        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
    }

    public static void setUpObjectForResultSetOperation(PreparedStatement _preparedStatement, ResultSet _resultSetInsert, ResultSet _resultSet, ResultSetMetaData _resultSetMetaData, int[] intArray)
            throws SQLException {
        Mockito.when(_preparedStatement.getGeneratedKeys()).thenReturn(_resultSetInsert);
        Mockito.when(_resultSetInsert.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(0);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(intArray);
    }

    /**
     * Returns a predefined LinkedHashSet containing column names used for testing database operations.
     * @return LinkedHashSet<String> A set of column names in the order they should appear
     */
    public static LinkedHashSet<String> getLinkedHashSet() {
        return new LinkedHashSet<>(Arrays.asList(
                "id", "sno", "name", "sdate", "stime",
                "distance", "salary", "bonus", "clob",
                "isqualified", "result"
        ));
    }

    public static Map<String, String> getDataTypeMap() {
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("id", "integer");
        dataTypeMap.put("sno", "integer");
        dataTypeMap.put("name", "string");
        dataTypeMap.put("sdate", "date");
        dataTypeMap.put("stime", "time");
        dataTypeMap.put("distance", "long");
        dataTypeMap.put("salary", "float");
        dataTypeMap.put("bonus", "double");
        dataTypeMap.put("clob", "BLOB");
        dataTypeMap.put("isqualified", "boolean");
        dataTypeMap.put("result", "string");
        return dataTypeMap;
    }

    public static boolean verifyTestForNullSetExecute(PreparedStatement _preparedStatement) throws SQLException {
        //verification on null value passed
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(2, Types.INTEGER);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(3, Types.VARCHAR);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(4, Types.DATE);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(5, Types.TIME);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(6, Types.BIGINT);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(7, Types.FLOAT);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(8, Types.DECIMAL);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(9, Types.BLOB);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(10, Types.BOOLEAN);

        //verification on value not null
        Mockito.verify(_preparedStatement, Mockito.times(0)).setNull(1, Types.INTEGER);
        Mockito.verify(_preparedStatement, Mockito.times(0)).setNull(11, Types.VARCHAR);

        return true;
    }

    /**
     * This method will set the data for set data for Unique constraint exception
     *
     * @param _updateRequest
     * @param simpleOperationResponse
     * @param _preparedStatement
     * @param _resultSet
     * @param _databaseMetaData
     * @param _propertyMap
     */
    public static void setConstraintExceptionOperation(UpdateRequest _updateRequest, SimpleOperationResponse simpleOperationResponse,
                                                       PreparedStatement _preparedStatement, ResultSet _resultSet,
                                                       DatabaseMetaData _databaseMetaData, PropertyMap _propertyMap)
            throws SQLException {
        BatchUpdateException bac = new BatchUpdateException(CONSTRAINT_EXCEPTION_MESSAGE, new int[]{1,2});
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeBatch()).thenThrow(bac);
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForInteger(_resultSet);
    }

    /**
     * Mocks update request to return multiple docs.
     * @param _preparedStatement the prepared statement
     * @param _resultSetInsert the result set for insert
     * @param _resultSet the result set
     * @param _resultSetMetaData the result set metadata
     * @param intArray the integer array
     *
     * @throws SQLException sql exception
     */
    public static void setUpObjectForResultSetMultipleDocsOperation(PreparedStatement _preparedStatement,
                                                                  ResultSet _resultSetInsert, ResultSet _resultSet,
                                                                  ResultSetMetaData _resultSetMetaData, int[] intArray)
            throws SQLException {
        Mockito.when(_preparedStatement.getGeneratedKeys()).thenReturn(_resultSetInsert);
        Mockito.when(_resultSetInsert.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(0);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(intArray);
    }
}
