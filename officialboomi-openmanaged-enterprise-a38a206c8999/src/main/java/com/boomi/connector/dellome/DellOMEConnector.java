// Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.dellome;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.util.BaseConnector;

public class DellOMEConnector extends BaseConnector {

    @Override
    public Browser createBrowser(BrowseContext context) {
        return new DellOMEBrowser(createConnection(context));
    }    

    @Override
    protected Operation createQueryOperation(OperationContext context) {
        return new DellOMEQueryOperation(createConnection(context));
    }

    private DellOMEConnection createConnection(BrowseContext context) {
        return new DellOMEConnection(context);
    }
}