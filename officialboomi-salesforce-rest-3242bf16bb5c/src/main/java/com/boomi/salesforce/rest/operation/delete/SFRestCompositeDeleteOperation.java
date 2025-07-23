// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.delete;

import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.util.BaseDeleteOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.composite.SFCompositeCollectionDelete;
import com.boomi.util.IOUtil;

/**
 * Implementation of DELETE operation that processes the input documents using Salesforce Composite API.
 */
public class SFRestCompositeDeleteOperation extends BaseDeleteOperation {

    SFRestCompositeDeleteOperation(SFRestConnection conn) {
        super(conn);
    }

    @Override
    protected void executeDelete(DeleteRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());
            new SFCompositeCollectionDelete(getConnection(), request, response,
                    getContext().getConfig()).startCompositeDelete();
        } finally {
            IOUtil.closeQuietly(getConnection());
        }
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}
