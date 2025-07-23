// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.operation.OperationFactory;
import com.boomi.salesforce.rest.operation.SFRestCSVBulkV2Operation;
import com.boomi.salesforce.rest.operation.SFRestCustomSOQLOperation;
import com.boomi.salesforce.rest.operation.SFRestQueryOperation;

public class SFRestConnector extends BaseConnector {

    @Override
    public Browser createBrowser(BrowseContext context) {
        return new SFRestBrowser(createConnection(context));
    }

    @Override
    protected Operation createQueryOperation(OperationContext context) {
        return new SFRestQueryOperation(createConnection(context));
    }

    @Override
    protected Operation createCreateOperation(OperationContext context) {
        return OperationFactory.createOperation(context);
    }

    @Override
    protected Operation createUpdateOperation(OperationContext context) {
        return OperationFactory.createOperation(context);
    }

    @Override
    protected Operation createDeleteOperation(OperationContext context) {
        return OperationFactory.createOperation(context);
    }

    @Override
    protected Operation createUpsertOperation(OperationContext context) {
        return OperationFactory.createOperation(context);
    }

    @Override
    protected Operation createExecuteOperation(OperationContext context) {
        if (Constants.BULK_CSV_CUSTOM_DESCRIPTOR.equals(context.getCustomOperationType())) {
            return new SFRestCSVBulkV2Operation(createConnection(context));
        }
        return new SFRestCustomSOQLOperation(createConnection(context));
    }

    SFRestConnection createConnection(BrowseContext context) {
        return new SFRestConnection(context);
    }
}