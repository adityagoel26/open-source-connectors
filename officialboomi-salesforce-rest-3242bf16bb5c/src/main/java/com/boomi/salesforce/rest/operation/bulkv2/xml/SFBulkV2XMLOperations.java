// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2.xml;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.operation.QueryController;

import java.io.InputStream;
import java.util.List;

/**
 * Handles Bulk API V2 Input/Output XML Operations
 */
public class SFBulkV2XMLOperations {
    private final SFBulkV2Batcher _batcher;
    private final SFBulkV2Combiner _combiner;
    private final SFBulkV2Executor _executor;
    private final OperationRequest _request;
    private final SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public SFBulkV2XMLOperations(SFRestConnection connectionManager, OperationRequest request,
                                 OperationResponse response) {
        _connectionManager = connectionManager;
        _batcher = new SFBulkV2Batcher(request, response, _connectionManager.getOperationProperties());
        _combiner = new SFBulkV2Combiner(_connectionManager, response);
        _executor = new SFBulkV2Executor(_connectionManager, response);
        _request = request;
    }

    /**
     * Starts the Bulk Delete operation
     */
    public void startDelete() {
        List<TrackedData> batchObjects = _batcher.nextDeleteBatch();
        while (!batchObjects.isEmpty()) {
            Pair<InputStream, Long> batchData = _combiner.writeDeleteBatch(batchObjects);

            if (batchData != null) {
                _executor.sendBulk(batchData.getKey(), batchData.getValue(), batchObjects);
            }

            batchObjects = _batcher.nextDeleteBatch();
        }
    }

    /**
     * Starts the Bulk Update/Create/Upsert operations
     */
    public void startUpdate() {
        List<TrackedData> batchObjects = _batcher.nextUpdateBatch();
        while (!batchObjects.isEmpty()) {
            Pair<InputStream, Long> batchData = _combiner.writeUpdateBatch(batchObjects);

            if (batchData != null) {
                _executor.sendBulk(batchData.getKey(), batchData.getValue(), batchObjects);
            }

            batchObjects = _batcher.nextUpdateBatch();
        }

    }

    /**
     * Starts the Bulk Update/Create/Upsert operations
     */
    public void startCreate() {
        startUpdate();
    }

    /**
     * Starts the Bulk Update/Create/Upsert operations
     */
    public void startUpsert() {
        startUpdate();
    }

    /**
     * Starts the Bulk Custom SOQL Query operation
     */
    public void startCustomSOQLQuery() {
        QueryController controller = new QueryController(_connectionManager);
        for (ObjectData requestData : (UpdateRequest) _request) {
            // readSOQLInput validates input length
            String queryString = controller.readSOQLInput(requestData);

            _executor.startQueryBulk(requestData, queryString);
        }
    }

    /**
     * Starts the Bulk Query operation
     */
    public void startQueryFilter(List<String> selectedFields) {
        FilterData filterData = ((QueryRequest) _request).getFilter();
        QueryFilter queryFilter = filterData.getFilter();

        // buildQueryString validates SOQL length
        String queryString = new QueryController(_connectionManager).buildQueryString(selectedFields, queryFilter);

        _executor.startQueryBulk(((QueryRequest) _request).getFilter(), queryString);
    }
}