// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.delete;

import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.OperationFactory;

/**
 * Factory class in charge of constructing DELETE Operations.
 */
public class DeleteOperationFactory extends OperationFactory {

    @Override
    protected Operation getOperation(OperationContext context) {
        if (isBulkOperation(context)) {
            return new SFBulkDeleteOperation(new SFRestConnection(context));
        }

        if (isCompositeOperation(context)) {
            return new SFRestCompositeDeleteOperation(new SFRestConnection(context));
        }

        return new SFRestDeleteOperation(new SFRestConnection(context));
    }
}
