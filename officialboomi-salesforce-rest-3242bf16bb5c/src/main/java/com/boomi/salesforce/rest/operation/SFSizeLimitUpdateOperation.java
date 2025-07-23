// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.SizeLimitedUpdateOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.util.IOUtil;

/**
 * This extension of {@link SizeLimitedUpdateOperation} takes care of initializing the {@link SFRestConnection}
 * before executing the operation, and to close it afterward
 */
public abstract class SFSizeLimitUpdateOperation extends SizeLimitedUpdateOperation {

    protected SFSizeLimitUpdateOperation(SFRestConnection connection) {
        super(connection);
    }

    /**
     * Execute the implementation update logic
     *
     * @param request  the filtered request
     * @param response the operation response
     */
    protected abstract void executeSFUpdate(UpdateRequest request, OperationResponse response);

    @Override
    protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());
            executeSFUpdate(request, response);
        } finally {
            IOUtil.closeQuietly(getConnection());
        }
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}
