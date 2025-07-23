// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.update;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.SFSizeLimitUpdateOperation;
import com.boomi.salesforce.rest.operation.composite.SFCompositeCollectionUpdate;

/**
 * Implementation of UPDATE operation that processes the input documents using Salesforce Composite API.
 */
public class SFRestCompositeUpdateOperation extends SFSizeLimitUpdateOperation {

    protected SFRestCompositeUpdateOperation(SFRestConnection connection) {
        super(connection);
    }

    @Override
    protected void executeSFUpdate(UpdateRequest request, OperationResponse response) {
        new SFCompositeCollectionUpdate(getConnection(), request, response,
                getContext().getConfig()).startCompositeOperation(getContext().getObjectTypeId());
    }
}
