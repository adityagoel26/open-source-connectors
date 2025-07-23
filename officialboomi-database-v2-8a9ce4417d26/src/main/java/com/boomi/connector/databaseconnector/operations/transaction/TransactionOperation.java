// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.transaction;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract class for Transaction Operations
 */
public abstract class TransactionOperation extends BaseUpdateOperation {
    private static final Logger LOG = LogUtil.getLogger(TransactionOperation.class);

    protected TransactionOperation(TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection) {
        super(transactionDatabaseConnectorConnection);
    }

    /**
     * Executes the transaction operationLogger responseLogger = operationResponse.getLogger();
     *
     * @param updateRequest
     * @param operationResponse
     */
    @Override
    protected final void executeUpdate(UpdateRequest updateRequest, OperationResponse operationResponse) {
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = getConnection();
        String topLevelExecutionId = updateRequest.getTopLevelExecutionId();
        TransactionCacheKey transactionCacheKey = transactionDatabaseConnectorConnection.createCacheKey(
                topLevelExecutionId);
        String transactionId = null;
        try {
            executeTransaction(transactionCacheKey);
            logOperationStatus(transactionCacheKey.toString(),
                    operationResponse.getLogger(), Level.INFO, getTransactionStatus());
            transactionId = transactionCacheKey.toString();

            PayloadMetadata metadata = createAndSetMetadata(operationResponse, transactionId, getTransactionStatus());
            ResponseUtil.addCombinedSuccess(operationResponse, updateRequest,
                    DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                    PayloadUtil.toPayload(StringUtil.EMPTY_STRING, metadata));

            logTransaction(transactionId, operationResponse.getLogger());
        } catch (ConnectorException e) {
            handleConnectorException(e, updateRequest, operationResponse);
        }
    }

    private static void handleConnectorException(ConnectorException e, UpdateRequest updateRequest,
            OperationResponse operationResponse) {
        //Adding Application Error with Connector Exception ErrorCode & Message
        operationResponse.addCombinedResult(updateRequest, OperationStatus.APPLICATION_ERROR,
                ConnectorException.getStatusCode(e), ConnectorException.getStatusMessage(e), null);
        LOG.log(Level.SEVERE, e.getMessage(), e);
    }

    /**
     * Executes the transaction operation
     *
     * @param transactionCacheKey
     */
    protected abstract void executeTransaction(TransactionCacheKey transactionCacheKey);

    /**
     * Logs the status of an operation related to a transaction.
     *
     * @param transactionId     The key or identifier for the transaction being processed.
     * @param responseLogger    The logger from OperationResponse to write to process log.
     * @param level             The logging level at which the operation status should be logged.
     * @param transactionStatus One of three values: "Started", "Committed" or "Rolled Back".
     */
    private static void logOperationStatus(String transactionId,
            Logger responseLogger, Level level, String transactionStatus) {
        String logMessage = String.format("Transaction %s %s", transactionId, transactionStatus);
        responseLogger.log(level, logMessage);
    }

    /**
     * Returns the transaction status
     *
     * @return
     */
    protected abstract String getTransactionStatus();

    private static PayloadMetadata createAndSetMetadata(OperationResponse operationResponse,
                                                        String transactionId, String transactionStatus) {
        PayloadMetadata metadata = operationResponse.createMetadata();
        metadata.setTrackedProperty(TransactionConstants.TRANSACTION_ID, transactionId);
        metadata.setTrackedProperty(TransactionConstants.TRANSACTION_STATUS, transactionStatus);
        return metadata;
    }

    private static void logTransaction(String transactionId, Logger responseLogger) {
        String logMessage = TransactionConstants.TRANSACTION_LOG_MESSAGE + transactionId;
        LOG.info(logMessage);
        responseLogger.info(logMessage);
    }

    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }
}