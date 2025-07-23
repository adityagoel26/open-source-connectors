// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.upsert;

import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.OperationFactory;

/**
 * Factory class in charge of constructing UPSERT Operations.
 */
public class UpsertOperationFactory extends OperationFactory {

    @Override
    protected Operation getOperation(OperationContext context) {
        if (isBulkOperation(context)) {
            return new SFBulkUpsertOperation(new SFRestConnection(context));
        }

        if (isCompositeOperation(context)) {
            return new SFRestCompositeUpsertOperation(new SFRestConnection(context));
        }

        return new SFRestUpsertOperation(new SFRestConnection(context));
    }
}
