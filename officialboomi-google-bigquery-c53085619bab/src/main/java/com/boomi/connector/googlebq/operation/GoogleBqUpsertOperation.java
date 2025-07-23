// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.upsert.PayloadResponseBuilder;
import com.boomi.connector.googlebq.operation.upsert.strategy.BaseStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.DeleteStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.LoadJobStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.QueryJobStrategy;
import com.boomi.connector.util.BaseUpdateOperation;

public class GoogleBqUpsertOperation extends BaseUpdateOperation {

    private static final String DELETE_TEMP_TABLE_AFTER_QUERY = "deleteTempTableAfterQuery";
    private static final String RUN_SQL_AFTER_LOAD = "runSqlAfterLoad";
    private static final String SUCCESS_CODE = "Success";

    /**
     * Constructs a new GoogleBqUpsertOperation instance.
     *
     * @param conn
     *         a {@link GoogleBqOperationConnection} instance.
     */
    public GoogleBqUpsertOperation(GoogleBqOperationConnection conn) {
        super(conn);
    }

    @Override
    protected void executeUpdate(UpdateRequest updateRequest, OperationResponse operationResponse) {
        try {
            for (ObjectData data : updateRequest) {
                handleJob(data, operationResponse);
            }
        } catch (Exception e) {
            // mark pending documents as Failure
            ResponseUtil.addExceptionFailures(operationResponse, updateRequest, e);
        }
    }

    private void handleJob(ObjectData data, OperationResponse operationResponse) {
        PayloadResponseBuilder resultBuilder = new PayloadResponseBuilder();
        GoogleBqOperationConnection connection = (GoogleBqOperationConnection) getConnection();
        boolean success;
        try {
            success = executeStrategy(new LoadJobStrategy(connection), resultBuilder, data);
            if (success && isRunSQLFieldSelected(data)) {
                success = executeStrategy(new QueryJobStrategy(connection), resultBuilder, data);
                if (shouldDeleteTempTable(data, success)) {
                    // if query step fails but the deletion needs to be executed, success flag should be false to
                    // ensure an application error
                    success &= executeStrategy(new DeleteStrategy(connection), resultBuilder, data);
                }
            }

            Payload payload = resultBuilder.toPayload();
            if (success) {
                ResponseUtil.addSuccess(operationResponse, data, SUCCESS_CODE, payload);
            } else {
                operationResponse.addResult(data, OperationStatus.APPLICATION_ERROR, resultBuilder.getCode(),
                        resultBuilder.getMessage(), payload);
            }
        } catch (Exception e) {
            // add the result for the current document
            ResponseUtil.addExceptionFailure(operationResponse, data, e);
            // and re throw the Exception
            throw e;
        }
    }

    private static boolean executeStrategy(BaseStrategy strategy, PayloadResponseBuilder builder, ObjectData data) {
        return strategy.execute(builder, data);
    }

    private static boolean shouldDeleteTempTable(TrackedData document, boolean isSuccessfulQuery) {
        String deleteTempTable = document.getDynamicOperationProperties().getProperty(DELETE_TEMP_TABLE_AFTER_QUERY);
        DeleteTable deleteTable = DeleteTable.valueOf(deleteTempTable);

        switch (deleteTable) {
            case DELETE_IF_SUCCESS_VALUE:
                return isSuccessfulQuery;
            case DELETE_ALWAYS_VALUE:
                return true;
            case NO_DELETE_VALUE:
                return false;
            default:
                throw new UnsupportedOperationException("unknown delete operation: " + deleteTempTable);
        }
    }

    private static boolean isRunSQLFieldSelected(TrackedData document) {
        return Boolean.parseBoolean(document.getDynamicOperationProperties().getProperty(RUN_SQL_AFTER_LOAD));
    }

    private enum DeleteTable {
        DELETE_ALWAYS_VALUE, DELETE_IF_SUCCESS_VALUE, NO_DELETE_VALUE
    }
}