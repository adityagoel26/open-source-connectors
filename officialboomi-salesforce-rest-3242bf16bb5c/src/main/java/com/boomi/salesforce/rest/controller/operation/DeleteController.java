// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.util.StringUtil;

public class DeleteController {
    private final SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public DeleteController(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    /**
     * Gets the Id tag of the sizeLimited XML inputData, then sends the DELETE request for the Id
     *
     * @param sobjectName the target SObject
     * @param recordId    the ID of the target record to be deleted
     * @throws ConnectorException if failed to DELETE
     */
    public void deleteREST(String sobjectName, String recordId) {
        if (StringUtil.isBlank(recordId)) {
            throw new ConnectorException("ID field is missing");
        }
        _connectionManager.getRequestHandler().executeDelete(sobjectName, recordId);
    }
}
