// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.connection;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.databaseconnector.cache.TransactionCache;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.util.ConnectorCache;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The class manages transaction for the database connector.
 * When the Start Transaction Operation is executed, a new database connection will be created and added to the cache
 * against the {@link TransactionCacheKey}
 * When the Commit Transaction Operation is executed, it will retrieve the connection object from the cache and
 * commit it.
 * When the Rollback Transaction Operation is executed, it will retrieve the connection object from the cache and
 * roll it back.
 */
public class TransactionDatabaseConnectorConnection extends DatabaseConnectorConnection<OperationContext> {

    public TransactionDatabaseConnectorConnection(OperationContext context) {
        super(context);
    }

    /**
     * Creates a new {@link Connection} and saves it in cache. If there is already an ongoing transaction then it
     * will throw an error.
     *
     * @param transactionCacheKey
     */
    public void startTransaction(TransactionCacheKey transactionCacheKey) {
        validateOperationType(OperationTypeConstants.START_TRANSACTION, getContext().getCustomOperationType());

        if (isOngoingTransaction(transactionCacheKey)) {
            throw new ConnectorException(TransactionConstants.ERR_ONGOING_TRAN,
                    "There is already an ongoing transaction for " + transactionCacheKey);
        }
        cacheTransaction(transactionCacheKey, super.getDatabaseConnection(), getContext());
    }

    /**
     * Commits the transaction by getting the {@link Connection} object from cache, closes the connection and clears
     * the cache.
     *
     * @param transactionCacheKey
     */
    public void commitTransaction(TransactionCacheKey transactionCacheKey) {
        validateOperationType(OperationTypeConstants.COMMIT_TRANSACTION, getContext().getCustomOperationType());
        Connection connection = getDatabaseConnection(transactionCacheKey);

        try {
            connection.commit();
            // Intentionally removed out of finally block because we don't want to close the connection/clear in case
            // of error.
            closeConnection(connection);
            cleanupCache(transactionCacheKey);
        } catch (SQLException e) {
            throw new ConnectorException("Error committing transaction", e);
        }
    }

    /**
     * Rolls back the transaction by getting the {@link Connection} object from cache, closes the connection and
     * clears the
     * cache.
     *
     * @param transactionCacheKey
     */
    public void rollbackTransaction(TransactionCacheKey transactionCacheKey) {
        validateOperationType(OperationTypeConstants.ROLLBACK_TRANSACTION, getContext().getCustomOperationType());
        Connection connection = getDatabaseConnection(transactionCacheKey);

        try {
            connection.rollback();
            // Intentionally removed out of finally block because we don't want to close the connection/clear in case
            // of error.
            closeConnection(connection);
            cleanupCache(transactionCacheKey);
        } catch (SQLException e) {
            throw new ConnectorException("Error rolling back the transaction", e);
        }
    }

    private static void validateOperationType(String operationConstant, String customOperationType) {
        if (!operationConstant.equals(customOperationType)) {
            throw new ConnectorException("This is not a " + operationConstant + " operation!");
        }
    }

    /**
     * Creates a {@link TransactionCacheKey} object and returns it.
     * @param topLevelExecutionId
     * @return
     */
    public TransactionCacheKey createCacheKey(String topLevelExecutionId) {
        return new TransactionCacheKey(topLevelExecutionId, getContext().getConnectionProperties());
    }

    /**
     * Validates if there is no existing transaction and the Connection properties.
     *
     * @param transactionCacheKey
     * @return
     */
    public Connection getDatabaseConnection(TransactionCacheKey transactionCacheKey) {

        if (!isOngoingTransaction(transactionCacheKey)) {
            throw new ConnectorException(TransactionConstants.ERR_NO_EXISTING_TRAN,
                    "There is no existing transaction for " + transactionCacheKey);
        }
        TransactionCache cache = getTransactionCache(transactionCacheKey);

        if (cache == null) {
            throw new ConnectorException(TransactionConstants.ERR_CACHE_KEY_NOT_FOUND,
                    "Transaction " + transactionCacheKey + " could not be found.");
        }
        return getConnectionFromCache(cache);
    }

    /**
     * Must reuse connection from cache
     * {@link TransactionDatabaseConnectorConnection#getDatabaseConnection(TransactionCacheKey)}
     *
     * @return
     */
    @Override
    public final Connection getDatabaseConnection() {
        throw new UnsupportedOperationException(
                "Cannot create a new connection from Transaction Operations");
    }

    /**
     * Get active connection from cache.
     *
     * @param cache
     * @return
     */
    private static Connection getConnectionFromCache(TransactionCache cache) {

        if (cache == null || cache.getConnection() == null) {
            return null;
        }

        Connection dbConnection;
        dbConnection = cache.getConnection();

        try {
            dbConnection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new ConnectorException("Failed to establish connection to the Database", e);
        }
        return dbConnection;
    }

    private boolean isOngoingTransaction(TransactionCacheKey transactionCacheKey) {
        TransactionCache cache = getTransactionCache(transactionCacheKey);
        return getConnectionFromCache(cache) != null;
    }

    private void cleanupCache(TransactionCacheKey transactionCacheKey) {
        ConnectorCache.clearCache(transactionCacheKey, getContext());
    }

    private static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new ConnectorException("Failed to close connection:", e);
            }
        }
    }

    private static void cacheTransaction(TransactionCacheKey transactionCacheKey, Connection databaseConnection,
            OperationContext context) {
         ConnectorCache.getCache(transactionCacheKey, context,
                TransactionCache.getConnectionFactory(databaseConnection));
    }

    private TransactionCache getTransactionCache(TransactionCacheKey transactionCacheKey) {
        return (TransactionCache) getContext().getConnectorCache().get(transactionCacheKey);
    }
}