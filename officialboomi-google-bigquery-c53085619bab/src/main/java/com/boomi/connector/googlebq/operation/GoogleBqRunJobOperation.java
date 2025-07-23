// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.GoogleBqObjectType;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.handler.RunJobHandler;
import com.boomi.connector.util.BaseUpdateOperation;

import java.util.logging.Level;

public class GoogleBqRunJobOperation extends BaseUpdateOperation {

    private RunJobHandler _jobHandler;

    public GoogleBqRunJobOperation(GoogleBqOperationConnection conn) {
        super(conn);
    }

    @Override
    protected void executeUpdate(UpdateRequest updateRequest, OperationResponse operationResponse) {
        try {
            GoogleBqOperationConnection connection = (GoogleBqOperationConnection) getConnection();
            // Validate the Job Type. if the Object Type is invalid, a ConnectorException is thrown
            String objectType = connection.getContext().getObjectTypeId();
            GoogleBqObjectType.fromString(objectType);
            _jobHandler = new RunJobHandler(connection);
            for (ObjectData data : updateRequest) {
                handleJob(data, operationResponse);
            }
        } catch (Exception e) {
            ResponseUtil.addExceptionFailures(operationResponse, updateRequest, e);
        }
    }

    private void handleJob(ObjectData data, OperationResponse operationResponse) {
        try {
            _jobHandler.runJob(data, operationResponse);
        } catch (ConnectorException e) {
            data.getLogger().log(Level.WARNING, e.getMessage(), e);
            operationResponse.addResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(),
                    e.getMessage(), null);
        } catch (Exception e) {
            ResponseUtil.addExceptionFailure(operationResponse, data, e);
        }
    }
}
