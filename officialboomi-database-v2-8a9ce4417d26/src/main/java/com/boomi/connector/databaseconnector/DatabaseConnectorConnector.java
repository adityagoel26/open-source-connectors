// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.databaseconnector.operations.StandardOperation;
import com.boomi.connector.databaseconnector.operations.StandardTransactionOperation;
import com.boomi.connector.databaseconnector.operations.delete.DynamicDeleteOperation;
import com.boomi.connector.databaseconnector.operations.delete.DynamicDeleteTransactionOperation;
import com.boomi.connector.databaseconnector.operations.get.DynamicGetOperation;
import com.boomi.connector.databaseconnector.operations.get.StandardGetOperation;
import com.boomi.connector.databaseconnector.operations.insert.DynamicInsertOperation;
import com.boomi.connector.databaseconnector.operations.insert.DynamicInsertTransactionOperation;
import com.boomi.connector.databaseconnector.operations.insert.StandardInsertOperation;
import com.boomi.connector.databaseconnector.operations.insert.StandardInsertTransactionOperation;
import com.boomi.connector.databaseconnector.operations.storedprocedureoperation.StoredProcedureOperation;
import com.boomi.connector.databaseconnector.operations.storedprocedureoperation.StoredProcedureTransactionOperation;
import com.boomi.connector.databaseconnector.operations.transaction.CommitOperation;
import com.boomi.connector.databaseconnector.operations.transaction.RollbackOperation;
import com.boomi.connector.databaseconnector.operations.transaction.StartTransactionOperation;
import com.boomi.connector.databaseconnector.operations.update.DynamicUpdateOperation;
import com.boomi.connector.databaseconnector.operations.update.DynamicUpdateTransactionOperation;
import com.boomi.connector.databaseconnector.operations.upsert.UpsertOperation;
import com.boomi.connector.databaseconnector.operations.upsert.UpsertTransactionOperation;
import com.boomi.connector.util.BaseConnector;

/**
 * The Class DatabaseConnectorConnector.
 *
 * @author swastik.vn
 */
public class DatabaseConnectorConnector extends BaseConnector {

    /**
     * Creates the browser.
     *
     * @param context the context
     * @return the browser
     */
    @Override
    public Browser createBrowser(BrowseContext context) {
        return new DatabaseConnectorBrowser(createConnection(context));
    }

    /**
     * Creates the create operation.
     *
     * @param context the context
     * @return the operation
     */
    @Override
    protected Operation createCreateOperation(OperationContext context) {
        String insertionType = (String) context.getOperationProperties().get(DatabaseConnectorConstants.INSERTION_TYPE);
        switch (insertionType) {
            case OperationTypeConstants.DYNAMIC_INSERT:

                if (isJoinTransaction(context)) {
                    return new DynamicInsertTransactionOperation(createTransactionConnection(context));
                }
                return new DynamicInsertOperation(createConnection(context));
            case OperationTypeConstants.STANDARD_INSERT:

                if (isJoinTransaction(context)) {
                    return new StandardInsertTransactionOperation(createTransactionConnection(context));
                }
                return new StandardInsertOperation(createConnection(context));
            default:
                throw new UnsupportedOperationException(
                        DatabaseConnectorConstants.OPERATION_TYPE_NOT_SUPPORTED + insertionType);
        }
    }

    /**
     * Creates the update operation.
     *
     * @param context the context
     * @return the operation
     */
    @Override
    protected Operation createUpdateOperation(OperationContext context) {
        String updateType = (String) context.getOperationProperties().get(DatabaseConnectorConstants.TYPE);
        boolean joinTransaction = isJoinTransaction(context);

        if (OperationTypeConstants.DYNAMIC_UPDATE.equals(updateType)) {
            return joinTransaction ? new DynamicUpdateTransactionOperation(createTransactionConnection(context))
                    : new DynamicUpdateOperation(createConnection(context));
        } else if (OperationTypeConstants.STANDARD_UPDATE.equals(updateType)) {
            return joinTransaction ? new StandardTransactionOperation(createTransactionConnection(context))
                    : new StandardOperation(createConnection(context));
        }
        throw new UnsupportedOperationException(DatabaseConnectorConstants.OPERATION_TYPE_NOT_SUPPORTED + updateType);
    }

    /**
     * Creates the execute operation.
     *
     * @param context the context
     * @return the operation
     */
    @Override
    protected Operation createExecuteOperation(OperationContext context) {
        String opsType = context.getCustomOperationType();
        String getType = (String) context.getOperationProperties().get(DatabaseConnectorConstants.GET_TYPE);
        String deleteType = (String) context.getOperationProperties().get(DatabaseConnectorConstants.DELETE_TYPE);
        boolean joinTransaction = isJoinTransaction(context);

        switch (opsType) {
            case DatabaseConnectorConstants.GET:

                if (OperationTypeConstants.DYNAMIC_GET.equals(getType)) {
                    return new DynamicGetOperation(createConnection(context));
                }
                return new StandardGetOperation(createConnection(context));
            case OperationTypeConstants.STOREDPROCEDUREWRITE:
                if (joinTransaction) {
                    return new StoredProcedureTransactionOperation(createTransactionConnection(context));
                }
                return new StoredProcedureOperation(createConnection(context));
            case DatabaseConnectorConstants.DELETE:

                if (OperationTypeConstants.DYNAMIC_DELETE.equals(deleteType)) {

                    if (joinTransaction) {
                        return new DynamicDeleteTransactionOperation(createTransactionConnection(context));
                    }
                    return new DynamicDeleteOperation(createConnection(context));
                }

                if (joinTransaction) {
                    return new StandardTransactionOperation(createTransactionConnection(context));
                }
                return new StandardOperation(createConnection(context));
            case OperationTypeConstants.START_TRANSACTION:
                return new StartTransactionOperation(createTransactionConnection(context));
            case OperationTypeConstants.COMMIT_TRANSACTION:
                return new CommitOperation(createTransactionConnection(context));
            case OperationTypeConstants.ROLLBACK_TRANSACTION:
                return new RollbackOperation(createTransactionConnection(context));
            default:
                throw new UnsupportedOperationException(
                        DatabaseConnectorConstants.OPERATION_TYPE_NOT_SUPPORTED + opsType);
        }
    }

    /**
     * Creates the upsert operation.
     *
     * @param context the context
     * @return the operation
     */
    @Override
    protected Operation createUpsertOperation(OperationContext context) {

        if (isJoinTransaction(context)) {
            return new UpsertTransactionOperation(createTransactionConnection(context));
        }
        return new UpsertOperation(createConnection(context));
    }

    /**
     * Creates the connection.
     *
     * @param context the context
     * @return the database connector connection
     */
    private static DatabaseConnectorConnection createConnection(BrowseContext context) {
        return new DatabaseConnectorConnection(context);
    }

    /**
     * Creates the {@link TransactionDatabaseConnectorConnection}
     * @param context
     * @return
     */
    private static TransactionDatabaseConnectorConnection createTransactionConnection(OperationContext context) {
        return new TransactionDatabaseConnectorConnection(context);
    }

    private static boolean isJoinTransaction(OperationContext context) {
        return context.getOperationProperties().getBooleanProperty(TransactionConstants.JOIN_TRANSACTION, false);
    }
}