// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The Class ProcedureMetaDataUtil.
 *
 * @author swastik.vn
 */
public class ProcedureMetaDataUtil {

    private static final int COLUMN_INDEX_TWO = 2;
    private static final int COLUMN_INDEX_FOUR = 4;
    private static final int COLUMN_INDEX_FIVE = 5;
    private static final int COLUMN_INDEX_SIX = 6;

    /**
     * Instantiates a new procedure meta data util.
     */
    private ProcedureMetaDataUtil() {

    }

    /**
     * This method will get the Input Parameters along with DataType required for
     * the procedure call. Condition has been added to differentiate the Stored procedure with same name
     * which are inside package and outside the package in case of Oracle DB. As "getProcedurecolumns" will fetch
     * all the columns irrespective of package in case of stored procedure outside package,
     * the condition has been added to check that.
     *
     * @param sqlConnection the connection
     * @param schemaName    the schema name
     * @param procedure     the procedure name
     * @param packageName   the package name of the procedure
     * @return the procedure metadata
     */

    public static Map<String, Integer> getProcedureMetadata(Connection sqlConnection, String procedure,
            String packageName, String schemaName) {
        Map<String, Integer> dataType = new HashMap<>();
        try {
            DatabaseMetaData md = sqlConnection.getMetaData();
            String schema = null;
            if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName()) && (packageName
                    != null)) {
                schema = packageName;
            } else {
                schema = sqlConnection.getCatalog();
            }
            try (ResultSet rs = md.getProcedureColumns(schema, schemaName, procedure, null);) {
                while (rs.next()) {
                    if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName())
                            && DatabaseConnectorConstants.UNKNOWN_DATATYPE.equals(rs.getString(COLUMN_INDEX_SIX))
                            && DatabaseConnectorConstants.JSON.equalsIgnoreCase(
                            rs.getString(DatabaseConnectorConstants.TYPE_NAME)) && (((rs.getString(
                            DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) != null) && rs.getString(
                            DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME).equals(packageName)) || (rs.getString(
                            DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) == null))) {
                        dataType.put(rs.getString(DatabaseConnectorConstants.COLUMN_NAME), Types.OTHER);
                    }
                    if (((DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(md.getDatabaseProductName())
                            && DatabaseConnectorConstants.UNKNOWN_DATATYPE.equals(rs.getString(COLUMN_INDEX_SIX))
                            && DatabaseConnectorConstants.JSON.equalsIgnoreCase(
                            rs.getString(DatabaseConnectorConstants.TYPE_NAME)))
                            || !DatabaseConnectorConstants.UNKNOWN_DATATYPE.equals(rs.getString(COLUMN_INDEX_SIX))) && (
                            rs.getString(DatabaseConnectorConstants.COLUMN_NAME) != null) && !"@RETURN_VALUE".equals(
                            rs.getString(DatabaseConnectorConstants.COLUMN_NAME))) {
                        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName())) {
                            if (((rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) != null)
                                    && rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME).equals(
                                    packageName)) || (rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)
                                    == null)) {
                                dataType.put(rs.getString(DatabaseConnectorConstants.COLUMN_NAME),
                                        Integer.valueOf(rs.getString(COLUMN_INDEX_SIX)));
                            }
                        } else {
                            dataType.put(rs.getString(DatabaseConnectorConstants.COLUMN_NAME),
                                    Integer.valueOf(rs.getString(COLUMN_INDEX_SIX)));
                        }
                    }
                    if (DatabaseConnectorConstants.REF_CURSOR.equals(
                            rs.getString(DatabaseConnectorConstants.TYPE_NAME))) {
                        dataType.put(rs.getString(DatabaseConnectorConstants.COLUMN_NAME), Types.REF_CURSOR);
                    }
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }

        return dataType;
    }

    /**
     * This method will get the Input Parameters required for the procedure call.
     *
     * @param sqlConnection the connection
     * @param schemaName    the schema name
     * @param procedure     the procedure name
     * @param packageName   the package name of the procedure
     * @return the procedure params
     */
    public static List<String> getProcedureParams(Connection sqlConnection, String procedure, String packageName,
            String schemaName) {

        List<String> params = new ArrayList<>();
        try {
            DatabaseMetaData md = sqlConnection.getMetaData();
            String databaseName = md.getDatabaseProductName();
            String schema = null;
            if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName()) && (packageName
                    != null)) {
                schema = packageName;
            } else {
                schema = sqlConnection.getCatalog();
            }
            try (ResultSet rs = md.getProcedureColumns(schema, schemaName, procedure, null);) {
                while (rs.next()) {

                    if ((DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase(databaseName)
                            && DatabaseConnectorConstants.UNKNOWN_DATATYPE.equals(rs.getString(COLUMN_INDEX_SIX))
                            && DatabaseConnectorConstants.JSON.equalsIgnoreCase(
                            rs.getString(DatabaseConnectorConstants.TYPE_NAME))) || (
                            !DatabaseConnectorConstants.UNKNOWN_DATATYPE.equals(rs.getString(COLUMN_INDEX_SIX)) && (
                                    rs.getString(DatabaseConnectorConstants.COLUMN_NAME) != null)
                                    && !"@RETURN_VALUE".equals(rs.getString(DatabaseConnectorConstants.COLUMN_NAME)))
                            || (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName())
                            && DatabaseConnectorConstants.UNKNOWN_DATATYPE.equals(rs.getString(COLUMN_INDEX_SIX))
                            && DatabaseConnectorConstants.JSON.equalsIgnoreCase(
                            rs.getString(DatabaseConnectorConstants.TYPE_NAME)))) {
                        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName())) {
                            if (((rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) != null)
                                    && rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME).equals(
                                    packageName)) || (rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)
                                    == null) || DatabaseConnectorConstants.REF_CURSOR.equals(
                                    rs.getString(DatabaseConnectorConstants.TYPE_NAME))) {
                                params.add(rs.getString(DatabaseConnectorConstants.COLUMN_NAME));
                            }
                        } else {
                            params.add(rs.getString(DatabaseConnectorConstants.COLUMN_NAME));
                        }
                    }
                    if (DatabaseConnectorConstants.REF_CURSOR.equals
                            (rs.getString(DatabaseConnectorConstants.TYPE_NAME))) {
                        params.add(rs.getString(DatabaseConnectorConstants.COLUMN_NAME));
                    }
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }

        return params;
    }

    /**
     * This method will get the IN params of Procedure.
     *
     * @param sqlConnection the connection
     * @param schemaName    the schema name
     * @param procedure     the procedure name
     * @param packageName   the package name of the procedure
     * @return the input params
     * @throws SQLException
     */
    public static List<String> getInputParams(Connection sqlConnection, String procedure, String packageName,
            String schemaName) throws SQLException {
        DatabaseMetaData md = sqlConnection.getMetaData();
        List<String> inparams = new ArrayList<>();
        String schema = null;
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName()) && (packageName != null)) {
            schema = packageName;
        } else {
            schema = sqlConnection.getCatalog();
        }
        try (ResultSet rs = sqlConnection.getMetaData().getProcedureColumns(schema, schemaName, procedure, null)) {
            while (rs.next()) {
                if (DatabaseConnectorConstants.MSSQLSERVER.equals(
                        sqlConnection.getMetaData().getDatabaseProductName())) {
                    if ((rs.getShort(COLUMN_INDEX_FIVE) == 1) || (rs.getShort(COLUMN_INDEX_FIVE) == COLUMN_INDEX_TWO)
                            || (rs.getShort(COLUMN_INDEX_FIVE) == COLUMN_INDEX_FOUR)) {
                        inparams.add(rs.getString(COLUMN_INDEX_FOUR));
                    }
                } else if (DatabaseConnectorConstants.ORACLE.equals(
                        sqlConnection.getMetaData().getDatabaseProductName())) {
                    if ((((rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) != null) && rs.getString(
                            DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME).equals(packageName)) || (rs.getString(
                            DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) == null)) && ((1 == rs.getShort(
                            COLUMN_INDEX_FIVE)) || (COLUMN_INDEX_TWO == rs.getShort(COLUMN_INDEX_FIVE)))) {
                        inparams.add(rs.getString(COLUMN_INDEX_FOUR));
                    }
                } else {
                    if ((rs.getShort(COLUMN_INDEX_FIVE) == 1) || (rs.getShort(COLUMN_INDEX_FIVE) == COLUMN_INDEX_TWO)) {
                        inparams.add(rs.getString(COLUMN_INDEX_FOUR));
                    }
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }

        return inparams;
    }

    /**
     * This method will get the OutPut Parameters of the Stored Procedure.
     *
     * @param sqlConnection the connection
     * @param schemaName    the schema name
     * @param procedure     the procedure name
     * @param packageName   the package name of the procedure
     * @return the output params
     * @throws SQLException
     */
    public static List<String> getOutputParams(Connection sqlConnection, String procedure, String packageName,
            String schemaName) throws SQLException {
        List<String> outParams = new ArrayList<>();
        DatabaseMetaData md = sqlConnection.getMetaData();
        String schema = null;
        if (DatabaseConnectorConstants.ORACLE.equalsIgnoreCase(md.getDatabaseProductName()) && (packageName != null)) {
            schema = packageName;
        } else {
            schema = sqlConnection.getCatalog();
        }
        try (ResultSet rs = sqlConnection.getMetaData().getProcedureColumns(schema, schemaName, procedure, null)) {
            while (rs.next()) {
                if (DatabaseConnectorConstants.ORACLE.equals(sqlConnection.getMetaData().getDatabaseProductName())) {
                    if (((((rs.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) != null) && rs.getString(
                            DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME).equals(packageName)) || (rs.getString(
                            DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) == null)) && (COLUMN_INDEX_TWO
                            == rs.getShort(COLUMN_INDEX_FIVE))) || (COLUMN_INDEX_FOUR == rs.getShort(
                            COLUMN_INDEX_FIVE))) {
                        outParams.add(rs.getString(COLUMN_INDEX_FOUR));
                    }
                } else if ((rs.getShort(COLUMN_INDEX_FIVE) == COLUMN_INDEX_TWO) || (rs.getShort(COLUMN_INDEX_FIVE)
                        == COLUMN_INDEX_FOUR)) {
                    outParams.add(rs.getString(COLUMN_INDEX_FOUR));
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }

        return outParams;
    }
}