// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.COLUMN_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TYPE_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.UNKNOWN_DATATYPE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcedureMetaDataUtilTest {

    private static final String SCHEMA_NAME = "EVENT";
    private static final String PROCEDURE = "Procedure Name";
    private static final String PACKAGE_NAME = "Package Name";
    private static final String SAMPLE_COLUMN_NAME = "Column Name";
    private static final String POST_GRE_SQL = "PostGreSQL";
    private static final String POST_GRE_SQL_SCHEMA_NAME = "PostGreSQL Schema Name";
    private static final String MSSQL_SERVER_INPUT_PARAMS = "MSSQLServer InputParams";
    private static final String ORACLE_INPUT_PARAMS = "Oracle InputParams";
    private static final String ELSE_CONDITION_INPUT_PARAMS = "ElseCondition InputParams";
    private static final String ORACLE_OUTPUT_PARAMS = "Oracle OutputParams";
    private static final String ELSE_IF_CONDITION_OUTPUT_PARAMS = "ElseIfCondition OutputParams";
    private final Connection _connection = mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = mock(DatabaseMetaData.class);
    private final ResultSet _resultSet = mock(ResultSet.class);

    @Before
    public void setup() throws SQLException {

        when(_connection.getMetaData()).thenReturn(_databaseMetaData);
    }

    private void setupDataForProcedureColumns(String oracle, String packageName) throws SQLException {

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(oracle);
        when(_databaseMetaData.getProcedureColumns(packageName, SCHEMA_NAME, PROCEDURE, null)).thenReturn(_resultSet);
    }

    private void setupDataForResultSet(String unknownDatatype) throws SQLException {

        when(_resultSet.getString(COLUMN_NAME)).thenReturn(SAMPLE_COLUMN_NAME);
        when(_resultSet.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        setupResultSetForProcedurePackageName();
        when(_resultSet.getString(6)).thenReturn(unknownDatatype);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);
    }

    private void setupResultSetForInputAndOutputParams(int x, String oracleInputParams) throws SQLException {

        when(_resultSet.getShort(5)).thenReturn((short) x);
        when(_resultSet.getString(4)).thenReturn(oracleInputParams);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);
    }

    private void setupForConnectionGetCatalog() throws SQLException {

        when(_connection.getCatalog()).thenReturn(POST_GRE_SQL_SCHEMA_NAME);
    }

    private void setupResultSetForProcedurePackageName() throws SQLException {

        when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn(PACKAGE_NAME);
    }

    @Test
    public void testGetProcedureMetadataOracleDb() throws SQLException {

        Map<String, Integer> expectedDataType = new HashMap<>();
        expectedDataType.put(SAMPLE_COLUMN_NAME, Integer.valueOf(UNKNOWN_DATATYPE));
        setupDataForProcedureColumns(DatabaseConnectorConstants.ORACLE, PACKAGE_NAME);
        setupDataForResultSet(UNKNOWN_DATATYPE);

        Map<String, Integer> actualDataType = ProcedureMetaDataUtil.getProcedureMetadata(_connection, PROCEDURE,
                PACKAGE_NAME, SCHEMA_NAME);

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
    }

    @Test
    public void testGetProcedureMetadataOracleNonUnknownType() throws SQLException {

        Map<String, Integer> expectedDataType = new HashMap<>();
        expectedDataType.put(SAMPLE_COLUMN_NAME, Integer.valueOf("1"));
        setupDataForProcedureColumns(DatabaseConnectorConstants.ORACLE, PACKAGE_NAME);
        setupDataForResultSet("1");

        Map<String, Integer> actualDataType = ProcedureMetaDataUtil.getProcedureMetadata(_connection, PROCEDURE,
                PACKAGE_NAME, SCHEMA_NAME);

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
    }

    @Test
    public void testGetProcedureMetadataForPostGreDb() throws SQLException {

        Map<String, Integer> expectedDataType = new HashMap<>();
        setupForConnectionGetCatalog();
        expectedDataType.put(SAMPLE_COLUMN_NAME, Integer.valueOf(UNKNOWN_DATATYPE));
        setupDataForProcedureColumns(POST_GRE_SQL, POST_GRE_SQL_SCHEMA_NAME);
        setupDataForResultSet(UNKNOWN_DATATYPE);

        Map<String, Integer> actualDataType = ProcedureMetaDataUtil.getProcedureMetadata(_connection, PROCEDURE,
                PACKAGE_NAME, SCHEMA_NAME);

        assertEquals(expectedDataType.keySet(), actualDataType.keySet());
    }

    @Test
    public void testGetProcedureParamsForPostGreDB() throws SQLException {

        List<String> expectedParams = Collections.singletonList(SAMPLE_COLUMN_NAME);
        setupForConnectionGetCatalog();
        setupDataForProcedureColumns(POST_GRE_SQL, POST_GRE_SQL_SCHEMA_NAME);
        setupDataForResultSet(UNKNOWN_DATATYPE);

        List<String> actualParams = ProcedureMetaDataUtil.getProcedureParams(_connection, PROCEDURE, PACKAGE_NAME,
                SCHEMA_NAME);

        assertEquals(expectedParams, actualParams);
    }

    @Test
    public void testGetProcedureParamsForOracleDB() throws SQLException {

        List<String> expectedParams = Collections.singletonList(SAMPLE_COLUMN_NAME);
        setupDataForProcedureColumns(DatabaseConnectorConstants.ORACLE, PACKAGE_NAME);
        setupDataForResultSet(UNKNOWN_DATATYPE);

        List<String> actualParams = ProcedureMetaDataUtil.getProcedureParams(_connection, PROCEDURE, PACKAGE_NAME,
                SCHEMA_NAME);

        assertEquals(expectedParams, actualParams);
    }

    @Test
    public void testGetInputParamsForMsSqlServerDB() throws SQLException {

        List<String> expectedInParams = Collections.singletonList(MSSQL_SERVER_INPUT_PARAMS);
        setupDataForProcedureColumns(DatabaseConnectorConstants.MSSQLSERVER, null);
        setupResultSetForProcedurePackageName();
        setupResultSetForInputAndOutputParams(1, MSSQL_SERVER_INPUT_PARAMS);

        List<String> actualInParams = ProcedureMetaDataUtil.getInputParams(_connection, PROCEDURE, PACKAGE_NAME,
                SCHEMA_NAME);

        assertEquals(expectedInParams, actualInParams);
    }

    @Test
    public void testGetInputParamsForOracleDB() throws SQLException {

        List<String> expectedInParams = Collections.singletonList(ORACLE_INPUT_PARAMS);
        setupDataForProcedureColumns(DatabaseConnectorConstants.ORACLE, PACKAGE_NAME);
        setupResultSetForProcedurePackageName();
        setupResultSetForInputAndOutputParams(1, ORACLE_INPUT_PARAMS);

        List<String> actualInParams = ProcedureMetaDataUtil.getInputParams(_connection, PROCEDURE, PACKAGE_NAME,
                SCHEMA_NAME);

        assertEquals(expectedInParams, actualInParams);
    }

    @Test
    public void testGetInputParamsForNonOracleDB() throws SQLException {

        List<String> expectedInParams = Collections.singletonList(ELSE_CONDITION_INPUT_PARAMS);
        setupForConnectionGetCatalog();
        setupDataForProcedureColumns(POST_GRE_SQL, POST_GRE_SQL_SCHEMA_NAME);
        setupResultSetForInputAndOutputParams(2, ELSE_CONDITION_INPUT_PARAMS);

        List<String> actualInParams = ProcedureMetaDataUtil.getInputParams(_connection, PROCEDURE, PACKAGE_NAME,
                SCHEMA_NAME);

        assertEquals(expectedInParams, actualInParams);
    }

    @Test
    public void testGetOutputParamsForOracleDB() throws SQLException {

        List<String> expectedOutParams = Collections.singletonList(ORACLE_OUTPUT_PARAMS);
        setupDataForProcedureColumns(DatabaseConnectorConstants.ORACLE, PACKAGE_NAME);
        setupResultSetForProcedurePackageName();
        setupResultSetForInputAndOutputParams(2, ORACLE_OUTPUT_PARAMS);

        List<String> actualOutParams = ProcedureMetaDataUtil.getOutputParams(_connection, PROCEDURE, PACKAGE_NAME,
                SCHEMA_NAME);

        assertEquals(expectedOutParams, actualOutParams);
    }

    @Test
    public void testGetOutputParamsForNonOracleDB() throws SQLException {

        List<String> expectedOutParams = Collections.singletonList(ELSE_IF_CONDITION_OUTPUT_PARAMS);
        setupDataForProcedureColumns(DatabaseConnectorConstants.MSSQLSERVER, null);
        setupResultSetForInputAndOutputParams(2, ELSE_IF_CONDITION_OUTPUT_PARAMS);

        List<String> actualOutParams = ProcedureMetaDataUtil.getOutputParams(_connection, PROCEDURE, PACKAGE_NAME,
                SCHEMA_NAME);

        assertEquals(expectedOutParams, actualOutParams);
    }
}
