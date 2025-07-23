// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.create;

import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.operation.OperationFactory;

/**
 * Factory class in charge of constructing CREATE Operations.
 */
public class CreateOperationFactory extends OperationFactory {

    @Override
    protected Operation getOperation(OperationContext context) {
        if (Constants.CREATE_TREE_CUSTOM_DESCRIPTOR.equals(context.getCustomOperationType())) {
            return new SFRestCreateOperation(new SFRestConnection(context));
        }

        if (isBulkOperation(context)) {
            return new SFBulkCreateOperation(new SFRestConnection(context));
        }

        if (isCompositeOperation(context)) {
            return new SFRestCompositeCreateOperation(new SFRestConnection(context));
        }

        return new SFRestCreateOperation(new SFRestConnection(context));
    }
}
