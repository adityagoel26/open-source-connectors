// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.controller.operation.QueryController;
import com.boomi.salesforce.rest.data.SOQLXMLSplitter;
import com.boomi.salesforce.rest.operation.bulkv2.xml.SFBulkV2XMLOperations;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.StringUtil;

import java.util.List;

public class SFRestQueryOperation extends BaseQueryOperation {
    public SFRestQueryOperation(SFRestConnection conn) {
        super(conn);
    }

    /**
     * This function is called when QUERY operation gets called
     */
    @Override
    protected void executeQuery(QueryRequest request, OperationResponse response) {
        try {
            getConnection().initialize(response.getLogger());

            String sobjectName = getContext().getObjectTypeId();
            getConnection().getOperationProperties().setSObject(sobjectName);
            getConnection().getOperationProperties().initDynamicProperties(request.getFilter());

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
     * Executes the QUERY to Bulk API
     */
    private void executeBulkQuery(QueryRequest request, OperationResponse response) {
        boolean queryAll = getConnection().getOperationProperties().getQueryAll();
        if (queryAll) {
            getConnection().getOperationProperties().setBulkOperation(Constants.QUERY_ALL_BULKV2);
        } else {
            getConnection().getOperationProperties().setBulkOperation(Constants.QUERY_BULKV2);
        }

        new SFBulkV2XMLOperations(getConnection(), request, response)
                .startQueryFilter(getContext().getSelectedFields());
    }

    /**
     * Executes the QUERY to REST API
     */
    private void executeRestQuery(QueryRequest request, OperationResponse response) {
        FilterData filterData = request.getFilter();
        QueryFilter queryFilter = filterData.getFilter();
        List<String> selectedFields = getContext().getSelectedFields();
        QueryController controller = new QueryController(getConnection());
        SOQLXMLSplitter reader = null;
        String nextPageUrl = null;
        try {
            do {
                if (StringUtil.isNotBlank(nextPageUrl)) {
                    reader = controller.nextPageQuery(nextPageUrl);
                } else {
                    reader = controller.query(selectedFields, queryFilter);
                }

                for (Payload p : reader) {
                    response.addPartialResult(filterData, OperationStatus.SUCCESS, "200", "success", p);
                }
                nextPageUrl = reader.getNextPageUrl();
                XMLUtils.closeSplitterQuietly(reader);

            } while (nextPageUrl != null);
            response.finishPartialResult(filterData);

        } catch (Exception e) {
            response.addErrorResult(filterData, OperationStatus.FAILURE, "", e.getMessage(), e);
        } finally {
            XMLUtils.closeSplitterQuietly(reader);
        }
    }

    @Override
    public SFRestConnection getConnection() {
        return (SFRestConnection) super.getConnection();
    }
}