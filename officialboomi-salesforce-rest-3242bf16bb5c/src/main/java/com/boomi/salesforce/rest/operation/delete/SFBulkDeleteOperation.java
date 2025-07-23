// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.delete;

import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.util.BaseDeleteOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.operation.bulkv2.xml.SFBulkV2XMLOperations;
import com.boomi.util.IOUtil;

/**
 * Implementation of DELETE operation that processes the input documents using Salesforce BULK V2 API
 */
public class SFBulkDeleteOperation extends BaseDeleteOperation {

    protected SFBulkDeleteOperation(SFRestConnection conn) {
        super(conn);
    }

    @Override
    protected void executeDelete(DeleteRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());
            executeBulkDelete(request, response);
        } finally {
            IOUtil.closeQuietly(getConnection());
        }
    }

    private void executeBulkDelete(DeleteRequest request, OperationResponse response) {
        String sobjectName = getContext().getObjectTypeId();
        getConnection().getOperationProperties().setSObject(sobjectName);
        getConnection().getOperationProperties().setBulkOperation(Constants.DELETE_BULKV2);

        new SFBulkV2XMLOperations(getConnection(), request, response).startDelete();
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}
