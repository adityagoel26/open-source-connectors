// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.create;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.operation.SFSizeLimitUpdateOperation;
import com.boomi.salesforce.rest.operation.bulkv2.xml.SFBulkV2XMLOperations;

/**
 * Implementation of CREATE operation that processes the input documents using Salesforce BULK V2 API
 */
public class SFBulkCreateOperation extends SFSizeLimitUpdateOperation {

    SFBulkCreateOperation(SFRestConnection conn) {
        super(conn);
    }

    @Override
    protected void executeSFUpdate(UpdateRequest request, OperationResponse response) {
        String sobjectName = getContext().getObjectTypeId();
        getConnection().getOperationProperties().setSObject(sobjectName);

        getConnection().getOperationProperties().setBulkOperation(Constants.INSERT_BULKV2);

        new SFBulkV2XMLOperations(getConnection(), request, response).startCreate();
    }
}
