// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.googlebq.browse.QueryResultsOpBrowser;
import com.boomi.connector.googlebq.browse.RunJobOpBrowser;
import com.boomi.connector.googlebq.browse.StreamingOpBrowser;
import com.boomi.connector.googlebq.browse.UpdateOpBrowser;
import com.boomi.connector.googlebq.browse.UpsertOpBrowser;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.GoogleBqQueryResultsOperation;
import com.boomi.connector.googlebq.operation.GoogleBqRunJobOperation;
import com.boomi.connector.googlebq.operation.GoogleBqStreamingOperation;
import com.boomi.connector.googlebq.operation.GoogleBqUpdateOperation;
import com.boomi.connector.googlebq.operation.GoogleBqUpsertOperation;
import com.boomi.connector.util.BaseConnector;
import com.boomi.util.StringUtil;

public class GoogleBqConnector extends BaseConnector {

    @Override
    public Browser createBrowser(BrowseContext browseContext) {
        String customOpType = browseContext.getCustomOperationType();
        if (StringUtil.isEmpty(customOpType)) {
            throw new UnsupportedOperationException(GoogleBqConstants.ERROR_EMPTY_CUSTOM_OPERATION_TYPE);
        }

        switch (customOpType) {
        case GoogleBqConstants.CUSTOM_OP_STREAMING:
            return new StreamingOpBrowser(new GoogleBqBaseConnection<>(browseContext));
        case GoogleBqConstants.CUSTOM_OP_RUN_JOB:
            return new RunJobOpBrowser(new GoogleBqBaseConnection<>(browseContext));
        case GoogleBqConstants.CUSTOM_OP_QUERY_RESULTS:
            return new QueryResultsOpBrowser(new GoogleBqBaseConnection<>(browseContext));
        case GoogleBqConstants.CUSTOM_OP_UPDATE:
        case GoogleBqConstants.CUSTOM_OP_TEST:
            return new UpdateOpBrowser(new GoogleBqBaseConnection<>(browseContext));
        case GoogleBqConstants.CUSTOM_OP_UPSERT:
            return new UpsertOpBrowser(new GoogleBqBaseConnection<>(browseContext));
        default:
            throw new UnsupportedOperationException(
                    String.format(GoogleBqConstants.ERROR_UNSUPPORTED_OPERATION_TYPE, customOpType));
        }
    }

    @Override
    public Operation createCreateOperation(OperationContext operationContext) {
        return new GoogleBqStreamingOperation(new GoogleBqOperationConnection(operationContext));
    }

    @Override
    public Operation createUpdateOperation(OperationContext operationContext) {
        return new GoogleBqUpdateOperation(new GoogleBqOperationConnection(operationContext));
    }

    @Override
    public Operation createUpsertOperation(OperationContext operationContext) {
        return new GoogleBqUpsertOperation(new GoogleBqOperationConnection(operationContext));
    }

    @Override
    public Operation createExecuteOperation(OperationContext operationContext) {
        String customOpType = operationContext.getCustomOperationType();
        switch (customOpType) {
        case GoogleBqConstants.CUSTOM_OP_RUN_JOB:
            return new GoogleBqRunJobOperation(new GoogleBqOperationConnection(operationContext));
        case GoogleBqConstants.CUSTOM_OP_QUERY_RESULTS:
            return new GoogleBqQueryResultsOperation(new GoogleBqOperationConnection(operationContext));
        default:
            throw new UnsupportedOperationException(
                    String.format(GoogleBqConstants.ERROR_UNSUPPORTED_OPERATION_TYPE, customOpType));
        }
    }
}
