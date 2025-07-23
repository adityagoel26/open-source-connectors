// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.update;

import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.OperationFactory;

/**
 * Factory class in charge of constructing UPDATE Operations.
 */
public class UpdateOperationFactory extends OperationFactory {

    @Override
    protected Operation getOperation(OperationContext context) {

        if (isBulkOperation(context)) {
            return new SFBulkUpdateOperation(new SFRestConnection(context));
        }

        if (isCompositeOperation(context)) {
            return new SFRestCompositeUpdateOperation(new SFRestConnection(context));
        }

        return new SFRestUpdateOperation(new SFRestConnection(context));
    }
}
