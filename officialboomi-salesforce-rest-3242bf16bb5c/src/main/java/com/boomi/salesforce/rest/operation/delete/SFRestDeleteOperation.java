// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.delete;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.util.BaseDeleteOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.DocumentController;
import com.boomi.salesforce.rest.controller.operation.DeleteController;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of DELETE operation that processes the input documents using Salesforce REST API.
 */
public class SFRestDeleteOperation extends BaseDeleteOperation {

    private static final Logger LOG = LogUtil.getLogger(SFRestDeleteOperation.class);

    SFRestDeleteOperation(SFRestConnection conn) {
        super(conn);
    }

    @Override
    protected void executeDelete(DeleteRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());
            executeRestDelete(request, response);
        } finally {
            IOUtil.closeQuietly(getConnection());
        }
    }

    private void executeRestDelete(DeleteRequest request, OperationResponse response) {
        DeleteController controller = new DeleteController(getConnection());
        String sobjectName = getContext().getObjectTypeId();
        for (ObjectIdData input : request) {
            getConnection().getOperationProperties().initDynamicProperties(input);

            String recordID = input.getObjectId();
            try {
                controller.deleteREST(sobjectName, recordID);
                response.addResult(input, OperationStatus.SUCCESS, "204", "success",
                        DocumentController.generateSuccessOutput(recordID));
            } catch (ConnectorException e) {
                LOG.log(Level.INFO, e, e::getMessage);
                response.addResult(input, OperationStatus.APPLICATION_ERROR, "", e.getMessage(),
                        DocumentController.generateFailedOutput(recordID, e.getMessage()));
            }
        }
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}
