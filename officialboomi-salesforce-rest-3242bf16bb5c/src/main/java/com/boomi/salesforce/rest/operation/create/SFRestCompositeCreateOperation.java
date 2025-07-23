// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.create;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.SFSizeLimitUpdateOperation;
import com.boomi.salesforce.rest.operation.composite.SFCompositeCollectionCreate;

/**
 * Implementation of CREATE operation that processes the input documents using Salesforce Composite API.
 */
public class SFRestCompositeCreateOperation extends SFSizeLimitUpdateOperation {

    SFRestCompositeCreateOperation(SFRestConnection conn) {
        super(conn);
    }

    @Override
    protected void executeSFUpdate(UpdateRequest request, OperationResponse response) {
        new SFCompositeCollectionCreate(getConnection(), request, response,
                getContext().getConfig()).startCompositeOperation(getContext().getObjectTypeId());
    }
}
