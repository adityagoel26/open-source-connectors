// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.connection;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.databaseconnector.cache.TransactionCache;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.testutil.DatabaseConnectorTestContext;
import com.boomi.connector.testutil.ResponseUtil;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Class to test {@link TransactionDatabaseConnectorConnection}
 */
public class TransactionDatabaseConnectorConnectionTest {
    
    private final Connection _connection = Mockito.mock(Connection.class);
    private final TransactionCacheKey _transactionCacheKey = new TransactionCacheKey(ResponseUtil.EXECUTION_ID, null);

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#getDatabaseConnection(TransactionCacheKey)} method.
     */
    @Test
    public void testJoinTransaction() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, null));
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.getDatabaseConnection(_transactionCacheKey);
            Assert.fail();
        } catch(ConnectorException e){
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId(348359912)"));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#getDatabaseConnection(TransactionCacheKey)} method.
     */
    @Test
    public void testGetConnection() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(new TransactionCache(_transactionCacheKey, _connection));
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = new TransactionDatabaseConnectorConnection(_operationContext);
        Connection connection = transactionDatabaseConnectorConnection.getDatabaseConnection(_transactionCacheKey);
        Assert.assertNotNull(connection);
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#getDatabaseConnection(TransactionCacheKey)} method when cache is null.
     */
    @Test
    public void testGetConnectionCacheNull() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, null));
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.getDatabaseConnection(_transactionCacheKey);
            Assert.fail();
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId(348359912)"));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#getDatabaseConnection(TransactionCacheKey)} method when connection is null.
     */
    @Test
    public void testgetSQLConnectionConnectionNull() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, null));
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.getDatabaseConnection(_transactionCacheKey);
            Assert.fail();
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId(348359912)"));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#getDatabaseConnection(TransactionCacheKey)} method when error setting autocommit to false.
     */
    @Test
    public void testgetSQLConnectionFailedToSetAutoCommit() throws SQLException {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(new TransactionCache(_transactionCacheKey, _connection));
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        Mockito.doThrow(new SQLException("Mocked SQL exception!")).when(_connection).setAutoCommit(false);
        try {
            Connection connection = transactionDatabaseConnectorConnection.getDatabaseConnection(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertEquals("Failed to establish connection to the Database: java.sql.SQLException: Mocked SQL exception!", e.getMessage());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#startTransaction(TransactionCacheKey)} method.
     */
    @Test
    @Ignore("not able to mock Driver.connect")
    public void testStartTransaction() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(new TransactionCache(_transactionCacheKey, _connection));
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = Mockito.spy(
                new TransactionDatabaseConnectorConnection(_operationContext));
        Mockito.when(transactionDatabaseConnectorConnection.getContext()).thenReturn(_operationContext);
        try {
            Mockito.doReturn(_connection).when((DatabaseConnectorConnection) transactionDatabaseConnectorConnection).getDatabaseConnection();
        } catch (UnsupportedOperationException e) {
            //Do nothing
        }
        Mockito.when(_operationContext.getCustomOperationType()).thenReturn(OperationTypeConstants.START_TRANSACTION);

        try {
            transactionDatabaseConnectorConnection.startTransaction(_transactionCacheKey);
        } catch (Exception e) {
            Assert.fail("Test failed because of an exception:" + e.getMessage() + "\n" + e.getCause());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#startTransaction(TransactionCacheKey)} method when operation is not START TRANSACTION.
     */
    @Test
    public void testStartTransactionDiffOperation() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(new TransactionCache(_transactionCacheKey, _connection));
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.startTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertEquals("This is not a START_TRANSACTION operation!", e.getMessage());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#startTransaction(TransactionCacheKey)} method when there is an ongoing operation.
     */
    @Test
    public void testStartTransactionOngoingTrans() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.START_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.startTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is already an ongoing transaction for TransactionId(348359912)"));
            Assert.assertTrue(e.getStatusCode().contains(TransactionConstants.ERR_ONGOING_TRAN));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#commitTransaction(TransactionCacheKey)} method.
     */
    @Test
    public void testCommitTransaction() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.COMMIT_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = Mockito.spy(
                new TransactionDatabaseConnectorConnection(_operationContext));
        try {
            transactionDatabaseConnectorConnection.commitTransaction(_transactionCacheKey);
        } catch (Exception e) {
            Assert.fail("Test failed because of an exception:" + e.getMessage() + "\n" + e.getCause());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#commitTransaction(TransactionCacheKey)} method when operation is diff.
     */
    @Test
    public void testCommitTransactionDiffOperation() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.ROLLBACK_TRANSACTION);

        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.commitTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertEquals("This is not a COMMIT_TRANSACTION operation!", e.getMessage());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#commitTransaction(TransactionCacheKey)} method when there is no ongoing transaction.
     */
    @Test
    public void testCommitTransactionNoOnGoingTrans() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, null), OperationTypeConstants.COMMIT_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.commitTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId(348359912)"));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#commitTransaction(TransactionCacheKey)} method when connection props don't match.
     */
    @Test
    public void testCommitTransactionPropsDontMatch() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, null), OperationTypeConstants.COMMIT_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.commitTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId(348359912)"));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#commitTransaction(TransactionCacheKey)}  method when commit throws error.
     */
    @Test
    public void testCommitTransactionCommitThrows() throws SQLException {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.COMMIT_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        Mockito.doThrow(new SQLException("Mocked SQL exception!")).when(_connection).commit();

        try {
            transactionDatabaseConnectorConnection.commitTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertEquals("Error committing transaction: java.sql.SQLException: Mocked SQL exception!", e.getMessage());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#rollbackTransaction(TransactionCacheKey)}  method.
     */
    @Test
    public void testRollbackTransaction() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.ROLLBACK_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.rollbackTransaction(_transactionCacheKey);
        } catch (Exception e) {
            Assert.fail("Test failed because of an exception:" + e.getMessage() + "\n" + e.getCause());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#rollbackTransaction(TransactionCacheKey)}  method when operation is diff.
     */
    @Test
    public void testRollbackTransactionDiffOperation() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.START_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.rollbackTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertEquals("This is not a ROLLBACK_TRANSACTION operation!", e.getMessage());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#rollbackTransaction(TransactionCacheKey)}  method when there is no ongoing transaction.
     */
    @Test
    public void testRollbackTransactionNoOnGoingTrans() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, null), OperationTypeConstants.ROLLBACK_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.rollbackTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId(348359912)"));
            Assert.assertTrue(e.getStatusCode().contains(TransactionConstants.ERR_NO_EXISTING_TRAN));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#rollbackTransaction(TransactionCacheKey)}  method when connection props don't match.
     */
    @Test
    public void testRollbackTransactionPropsDontMatch() {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, null), OperationTypeConstants.ROLLBACK_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        try {
            transactionDatabaseConnectorConnection.rollbackTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId(348359912)"));
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#rollbackTransaction(TransactionCacheKey)}  method when rollback throws error.
     */
    @Test
    public void testRollbackTransactionRollbackThrows() throws SQLException {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.ROLLBACK_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        Mockito.doThrow(new SQLException("Mocked SQL exception!")).when(_connection).rollback();
        try {
            transactionDatabaseConnectorConnection.rollbackTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertEquals("Error rolling back the transaction: java.sql.SQLException: Mocked SQL exception!", e.getMessage());
        }
    }

    /**
     * Tests the {@link TransactionDatabaseConnectorConnection#rollbackTransaction(TransactionCacheKey)} method when close throws error.
     */
    @Test
    public void testRollbackTransactionCloseThrows() throws SQLException {
        OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext(
                new TransactionCache(_transactionCacheKey, _connection), OperationTypeConstants.ROLLBACK_TRANSACTION);
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection =
                new TransactionDatabaseConnectorConnection(_operationContext);
        Mockito.doThrow(new SQLException("Mocked SQL exception!")).when(_connection).close();
        try {
            transactionDatabaseConnectorConnection.rollbackTransaction(_transactionCacheKey);
            Assert.fail("Failed to throw error!");
        } catch (ConnectorException e) {
            Assert.assertEquals("Failed to close connection:: java.sql.SQLException: Mocked SQL exception!", e.getMessage());
        }
    }
}
