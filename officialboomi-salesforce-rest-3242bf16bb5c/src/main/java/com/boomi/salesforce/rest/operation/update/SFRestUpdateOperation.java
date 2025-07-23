// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.operation.update;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.DocumentController;
import com.boomi.salesforce.rest.controller.operation.UpdateController;
import com.boomi.salesforce.rest.data.BatchUpdatedResponse;
import com.boomi.salesforce.rest.data.BatchUpdatedResponseSplitter;
import com.boomi.salesforce.rest.operation.SFSizeLimitUpdateOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of UPDATE operation that processes the input documents using Salesforce REST API.
 */
public class SFRestUpdateOperation extends SFSizeLimitUpdateOperation {

    private static final Logger LOG = LogUtil.getLogger(SFRestUpdateOperation.class);

    public SFRestUpdateOperation(SFRestConnection conn) {
        super(conn);
    }

    /**
     * This function is called when UPDATE operation gets called.<br> Applies size input validation.
     */
    @Override
    protected void executeSFUpdate(UpdateRequest request, OperationResponse response) {
        if (getConnection().getOperationProperties().getReturnUpdatedRecord()) {
            updateRecords(request, response, SFRestUpdateOperation::executeUpdateAddResponse);
        } else {
            updateRecords(request, response, SFRestUpdateOperation::executeUpdateIgnoreResponse);
        }
    }

    private void updateRecords(UpdateRequest request, OperationResponse response, RecordUpdater recordUpdater) {
        for (ObjectData input : request) {
            getConnection().getOperationProperties().initDynamicProperties(input);
            InputStream inputData = null;
            String recordID = null;
            try {
                inputData = input.getData();
                UpdateController controller = new UpdateController(getConnection(), getContext().getObjectTypeId(),
                        inputData);
                recordID = controller.getRecordId();
                recordUpdater.execute(controller, response, input);
            } catch (Exception e) {
                LOG.log(Level.INFO, e, e::getMessage);
                response.addResult(input, OperationStatus.APPLICATION_ERROR, "", e.getMessage(),
                        DocumentController.generateFailedOutput(recordID, e.getMessage()));
            } finally {
                IOUtil.closeQuietly(inputData);
            }
        }
    }

    private static void executeUpdateIgnoreResponse(UpdateController controller, OperationResponse response,
            TrackedData input) {
        controller.updateREST();
        response.addResult(input, OperationStatus.SUCCESS, "204", "success",
                DocumentController.generateSuccessOutput(controller.getRecordId()));
    }

    private static void executeUpdateAddResponse(UpdateController controller, OperationResponse response,
            TrackedData input) {
        ClassicHttpResponse salesforceResponse = null;
        try {
            salesforceResponse = controller.updateReturnUpdatedRecord();

            BatchUpdatedResponseSplitter splitter = new BatchUpdatedResponseSplitter(salesforceResponse);
            BatchUpdatedResponse batchUpdatedResponse = splitter.processResponse();

            if (batchUpdatedResponse.hasErrors()) {
                response.addResult(input, OperationStatus.APPLICATION_ERROR, "", batchUpdatedResponse.getErrorMessage(),
                        DocumentController.generateFailedOutput(controller.getRecordId(),
                                batchUpdatedResponse.getErrorMessage()));
            } else {
                response.addResult(input, OperationStatus.SUCCESS, "201", "success",
                        PayloadUtil.toPayload(batchUpdatedResponse.getResponse()));
            }
        } catch (Exception e) {
            throw new ConnectorException(e);
        } finally {
            IOUtil.closeQuietly(salesforceResponse);
        }
    }

    @FunctionalInterface
    private interface RecordUpdater {

        void execute(UpdateController controller, OperationResponse response, TrackedData input);
    }
}