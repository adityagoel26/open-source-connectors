// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.openapi.OpenAPIConnector;
import com.boomi.connector.veeva.browser.VeevaBrowser;
import com.boomi.connector.veeva.operation.VeevaExecuteOperation;
import com.boomi.connector.veeva.operation.query.VeevaQueryOperation;

public class VeevaConnector extends OpenAPIConnector {

    @Override
    public Browser createBrowser(BrowseContext context) {
        return new VeevaBrowser(new VeevaConnection<>(context));
    }

    @Override
    protected Operation createQueryOperation(OperationContext context) {
        return new VeevaQueryOperation(new VeevaOperationConnection(context));
    }

    @Override
    protected Operation createExecuteOperation(OperationContext context) {
        return new VeevaExecuteOperation(new VeevaOperationConnection(context));
    }
}