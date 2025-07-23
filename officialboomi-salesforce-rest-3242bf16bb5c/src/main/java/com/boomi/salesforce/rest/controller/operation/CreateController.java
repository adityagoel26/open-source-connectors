// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.operation;

import com.boomi.salesforce.rest.SFRestConnection;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.InputStream;

public class CreateController {
    private final SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public CreateController(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    /**
     * Sends the request and get the response of salesforce CREATE requests
     *
     * @param sobjectName the target SObject name
     * @param inputData   the input data to be created
     * @return Payload contains the CREATE response
     * @throws ConnectorException if failed to CREATE
     */
    public ClassicHttpResponse createREST(String sobjectName, InputStream inputData, long contentLength) {
        String assignmentRuleID = _connectionManager.getOperationProperties().getAssignmentRuleId();
        return _connectionManager.getRequestHandler()
                                 .executeCreate(sobjectName, inputData, contentLength, assignmentRuleID);
    }


    /**
     * Sends the request and get the response of salesforce CREATE TREE requests
     *
     * @param sobjectName the target SObject name
     * @param inputData   the input data to be created
     * @return Payload contains the CREATE response
     * @throws ConnectorException if failed to CREATE
     */
    public ClassicHttpResponse createTree(String sobjectName, InputStream inputData, long contentLength) {
        String assignmentRuleID = _connectionManager.getOperationProperties().getAssignmentRuleId();
        return _connectionManager.getRequestHandler()
                                 .executeCreateTree(sobjectName, inputData, contentLength, assignmentRuleID);
    }
}
