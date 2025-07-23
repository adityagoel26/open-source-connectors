// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Common methods related to database
 */
public class DatabaseUtil {

    private DatabaseUtil() {
        // No instances needed
    }

    /**
     * Commits a connection.
     *
     * @param sqlConnection the _sqlConnection
     * @throws ConnectorException the connector exception
     */
    public static void commit(Connection sqlConnection) throws ConnectorException {
        try {
            sqlConnection.commit();
        } catch (SQLException e) {
            throw new ConnectorException("Commit Failed", e.getMessage(), e);
        }
    }

    /**
     * Setup _sqlConnection.
     *
     * @param context the context
     * @param sqlConnection     the _sqlConnection
     * @param databaseConnectorConnection    the databaseConnectorConnection
     * @throws SQLException the SQL exception
     */
    public static void setupConnection(OperationContext context, Connection sqlConnection,
            DatabaseConnectorConnection databaseConnectorConnection) throws SQLException {
        String schemaName = (String) context.getOperationProperties().get(DatabaseConnectorConstants.SCHEMA_NAME);
        QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName,
                databaseConnectorConnection.getSchemaName());
        sqlConnection.setAutoCommit(false);
    }
}
