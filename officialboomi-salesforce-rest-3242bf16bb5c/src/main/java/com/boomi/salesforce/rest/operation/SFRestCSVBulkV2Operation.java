// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.operation.bulkv2.csv.SFBulkV2CSVOperations;

public class SFRestCSVBulkV2Operation extends BaseUpdateOperation {
    public SFRestCSVBulkV2Operation(SFRestConnection conn) {
        super(conn);
    }

    /**
     * This function is called when CSVBulkV2 operation gets called.<br> Does not require Size Limited inputs<br>
     */
    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());

            String operation = getConnection().getOperationProperties().getBulkOperation();
            if (operation.equals(Constants.QUERY_BULKV2) || operation.equals(Constants.QUERY_ALL_BULKV2)) {
                new SFBulkV2CSVOperations(getConnection()).executeQuery(request, response);
            } else {
                new SFBulkV2CSVOperations(getConnection()).executeCUD(request, response);
            }
        } finally {
            getConnection().close();
        }
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}