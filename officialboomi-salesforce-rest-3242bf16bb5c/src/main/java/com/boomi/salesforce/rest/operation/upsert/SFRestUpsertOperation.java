// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.operation.upsert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.DocumentController;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of UPSERT operation that processes the input documents using Salesforce REST API.
 */
public class SFRestUpsertOperation extends BaseUpdateOperation {

    private static final Logger LOG = LogUtil.getLogger(SFRestUpsertOperation.class);

    public SFRestUpsertOperation(SFRestConnection conn) {
        super(conn);
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());
            executeUpsert(request, response);
        } finally {
            IOUtil.closeQuietly(getConnection());
        }
    }

    private void executeUpsert(UpdateRequest request, OperationResponse response) {
        SFRestConnection connection = getConnection();
        OperationProperties operationProperties = connection.getOperationProperties();
        
        String externalIdName = operationProperties.getExternalIdFieldName();
        if (StringUtil.isBlank(externalIdName)) {
            throw new ConnectorException("External Id Name not provided");
        }

        for (ObjectData input : request) {
            operationProperties.initDynamicProperties(input);

            String externalIdValue = operationProperties.getExternalIdValue();
            String assignmentRuleID = operationProperties.getAssignmentRuleId();

            InputStream inputData = null;
            ClassicHttpResponse salesforceResponse = null;
            try {
                inputData = input.getData();
                salesforceResponse = connection.getRequestHandler().executeUpsert(getContext().getObjectTypeId(),
                        externalIdName, externalIdValue, inputData, assignmentRuleID);
                String statusCode = String.valueOf(salesforceResponse.getCode());
                Payload payload = PayloadUtil.toPayload(SalesforceResponseUtil.getContent(salesforceResponse));
                response.addResult(input, OperationStatus.SUCCESS, statusCode, "success", payload);
            } catch (Exception e) {
                LOG.log(Level.INFO, e, e::getMessage);
                response.addResult(input, OperationStatus.APPLICATION_ERROR, "", e.getMessage(),
                        DocumentController.generateFailedOutput("", e.getMessage()));
            } finally {
                IOUtil.closeQuietly(inputData, salesforceResponse);
            }
        }
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}