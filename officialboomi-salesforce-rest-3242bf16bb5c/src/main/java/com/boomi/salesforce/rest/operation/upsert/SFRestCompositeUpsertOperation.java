// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.upsert;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.SFSizeLimitUpdateOperation;
import com.boomi.salesforce.rest.operation.composite.SFCompositeCollectionUpsert;

/**
 * Implementation of UPSERT operation that processes the input documents using Salesforce Composite API.
 */
public class SFRestCompositeUpsertOperation extends SFSizeLimitUpdateOperation {

    SFRestCompositeUpsertOperation(SFRestConnection connection) {
        super(connection);
    }

    @Override
    protected void executeSFUpdate(UpdateRequest request, OperationResponse response) {
        String sobjectID = getContext().getObjectTypeId();
        String externalIdField = getConnection().getOperationProperties().getExternalIdFieldName();

        new SFCompositeCollectionUpsert(getConnection(), request, response, getContext().getConfig(), sobjectID,
                externalIdField).startCompositeOperation(sobjectID);
    }
}
