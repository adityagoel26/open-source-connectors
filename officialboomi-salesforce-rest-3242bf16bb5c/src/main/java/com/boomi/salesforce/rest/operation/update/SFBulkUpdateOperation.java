// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.update;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.operation.SFSizeLimitUpdateOperation;
import com.boomi.salesforce.rest.operation.bulkv2.xml.SFBulkV2XMLOperations;

/**
 * Implementation of UPDATE operation that processes the input documents using Salesforce BULK V2 API
 */
public class SFBulkUpdateOperation extends SFSizeLimitUpdateOperation {

    protected SFBulkUpdateOperation(SFRestConnection connection) {
        super(connection);
    }

    @Override
    protected void executeSFUpdate(UpdateRequest request, OperationResponse response) {
        String sobjectName = getContext().getObjectTypeId();
        getConnection().getOperationProperties().setSObject(sobjectName);
        getConnection().getOperationProperties().setBulkOperation(Constants.UPDATE_BULKV2);
        new SFBulkV2XMLOperations(getConnection(), request, response).startUpdate();
    }
}
