// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.util.BaseUpdateOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.controller.operation.QueryController;
import com.boomi.salesforce.rest.data.SOQLXMLSplitter;
import com.boomi.salesforce.rest.operation.bulkv2.xml.SFBulkV2XMLOperations;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.StringUtil;

public class SFRestCustomSOQLOperation extends BaseUpdateOperation {
    public SFRestCustomSOQLOperation(SFRestConnection conn) {
        super(conn);
    }

    /**
     * This function is called when CustomSOQL operation gets called.<br> Applies size input validation.
     */
    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());

            String sobjectName = getContext().getObjectTypeId();
            getConnection().getOperationProperties().setSObject(sobjectName);

            if (getConnection().getOperationProperties().isBulkOperationAPI()) {
                executeBulkQuery(request, response);
            } else {
                executeRestQuery(request, response);
            }
        } finally {
            getConnection().close();
        }
    }

    /**
     * Executes the QUERY to REST API
     */
    private void executeRestQuery(UpdateRequest request, OperationResponse response) {
        QueryController controller = new QueryController(getConnection());
        for (ObjectData requestData : request) {
            getConnection().getOperationProperties().initDynamicProperties(requestData);

            SOQLXMLSplitter reader = null;
            String nextPageUrl = null;
            try {
                do {
                    if (StringUtil.isNotBlank(nextPageUrl)) {
                        reader = controller.nextPageQuery(nextPageUrl);
                    } else {
                        // validates input length
                        reader = controller.customQuery(requestData);
                    }

                    for (Payload p : reader) {
                        response.addPartialResult(requestData, OperationStatus.SUCCESS, "200", "success", p);
                    }
                    nextPageUrl = reader.getNextPageUrl();
                    XMLUtils.closeSplitterQuietly(reader);

                } while (nextPageUrl != null);
                response.finishPartialResult(requestData);

            } catch (Exception e) {
                response.addErrorResult(requestData, OperationStatus.FAILURE, "", "Salesforce failed to QUERY records",
                                        e);
            } finally {
                XMLUtils.closeSplitterQuietly(reader);
            }
        }
    }

    /**
     * Executes the QUERY to BULK API
     */
    private void executeBulkQuery(UpdateRequest request, OperationResponse response) {
        boolean queryAll = getConnection().getOperationProperties().getQueryAll();
        if (queryAll) {
            getConnection().getOperationProperties().setBulkOperation(Constants.QUERY_ALL_BULKV2);
        } else {
            getConnection().getOperationProperties().setBulkOperation(Constants.QUERY_BULKV2);
        }

        new SFBulkV2XMLOperations(getConnection(), request, response).startCustomSOQLQuery();
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}