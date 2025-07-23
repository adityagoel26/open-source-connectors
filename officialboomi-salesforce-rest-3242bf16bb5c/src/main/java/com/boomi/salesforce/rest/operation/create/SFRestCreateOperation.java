// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.operation.create;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.util.PayloadUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.controller.DocumentController;
import com.boomi.salesforce.rest.controller.operation.CreateController;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of CREATE operation that processes the input documents using Salesforce REST API.
 */
public class SFRestCreateOperation extends BaseUpdateOperation {

    private static final Logger LOG = LogUtil.getLogger(SFRestCreateOperation.class);

    SFRestCreateOperation(SFRestConnection conn) {
        super(conn);
    }

    /**
     * This function is called when CREATE operation gets called.<br> SizeLimited input validation applied on Create
     * with Bulk API only.
     */
    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());
            executeRestCreate(request, response);
        } finally {
            IOUtil.closeQuietly(getConnection());
        }
    }

    /**
     * Executes the CREATE to REST API. Does not require Size Limited inputs
     */
    void executeRestCreate(UpdateRequest request, OperationResponse response) {
        String sobjectName = getContext().getObjectTypeId();

        CreateController controller = new CreateController(getConnection());
        for (ObjectData input : request) {
            getConnection().getOperationProperties().initDynamicProperties(input);

            InputStream inputData = null;
            ClassicHttpResponse salesforceResponse = null;
            try {
                inputData = input.getData();
                long contentLength = getContentLength(input);

                if (Constants.CREATE_TREE_CUSTOM_DESCRIPTOR.equals(getContext().getCustomOperationType())) {
                    salesforceResponse = controller.createTree(sobjectName, inputData, contentLength);
                } else {
                    salesforceResponse = controller.createREST(sobjectName, inputData, contentLength);
                }
                response.addResult(input, OperationStatus.SUCCESS, "201", "success",
                        PayloadUtil.toPayload(SalesforceResponseUtil.getContent(salesforceResponse)));
            } catch (Exception e) {
                LOG.log(Level.INFO, e, e::getMessage);
                response.addResult(input, OperationStatus.APPLICATION_ERROR, "", e.getMessage(),
                        DocumentController.generateFailedOutput("", e.getMessage()));
            } finally {
                IOUtil.closeQuietly(inputData, salesforceResponse);
            }
        }
    }

    private static long getContentLength(ObjectData data) {
        long contentLength = -1;
        try {
            contentLength = data.getDataSize();
        } catch (Exception e) {
            LOG.log(Level.INFO, e, () -> "an error happened obtaining the object data size: " + e.getMessage());
        }

        return contentLength;
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}