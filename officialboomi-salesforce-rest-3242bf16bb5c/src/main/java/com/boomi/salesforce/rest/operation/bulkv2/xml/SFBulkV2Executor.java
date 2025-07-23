// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2.xml;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.TrackedData;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2CUDController;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2QueryController;
import com.boomi.salesforce.rest.controller.bulkv2.reader.BulkV2CUDReader;
import com.boomi.salesforce.rest.controller.bulkv2.reader.BulkV2QueryReader;
import com.boomi.salesforce.rest.operation.bulkv2.BulkManager;
import com.boomi.util.IOUtil;

import java.io.InputStream;
import java.util.List;

/**
 * Executes a CSV batch file and print the XML output response
 */
public class SFBulkV2Executor {
    private final OperationResponse _response;
    private final SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public SFBulkV2Executor(SFRestConnection connectionManager, OperationResponse response) {
        _response = response;
        _connectionManager = connectionManager;
    }

    /**
     * Builds new bulk job and uploads the given batch<br> Reads and add the successful and failed results<br>
     *
     * @param bulkBatch     InputStream contains the bulk batch CSV body
     * @param contentLength length of the CSV data in bytes
     * @param batchObjects  list of input documents in this batch of bulk
     */
    public void sendBulk(InputStream bulkBatch, Long contentLength, List<TrackedData> batchObjects) {
        BulkV2CUDReader bulkReader = null;
        try {
            BulkV2CUDController bulkController = new BulkV2CUDController(_connectionManager);
            // start the bulk load operation steps
            bulkController.startBulk(bulkBatch, contentLength);

            // waits for Salesforce to finish processing
            BulkManager.waitCUD(bulkController);

            // Read Bulk Job results
            bulkReader = new BulkV2CUDReader(_connectionManager, bulkController.getJobId(),
                                             bulkController.getTotalRecordsCount(),
                                             bulkController.getFailedRecordsCount());
            try {
                if (bulkReader.initSuccessResultStream()) {
                    while (bulkReader.hasNext()) {
                        _response.addPartialResult(batchObjects, OperationStatus.SUCCESS, "200", "success",
                                                   bulkReader.getNext());
                    }
                }
            } finally {
                // closes the success stream
                bulkReader.close();
            }

            if (bulkReader.initFailedResultStream()) {
                // read next will close the stream when it is done
                while (bulkReader.hasNext()) {
                    _response.addPartialResult(batchObjects, OperationStatus.APPLICATION_ERROR, "",
                                               bulkReader.getBulkErrorMessage(), bulkReader.getNext());
                }
            }
            _response.finishPartialResult(batchObjects);
        } finally {
            IOUtil.closeQuietly(bulkBatch);
            if (bulkReader != null) {
                bulkReader.close();
            }
        }
    }

    /**
     * Builds new Query bulk job with the given SOQL query string<br> Reads and add the successful results<br>
     *
     * @param requestData input document for this batch of bulk
     * @param queryString TrackedData contains the bulk batch CSV body
     */
    public void startQueryBulk(TrackedData requestData, String queryString) {
        BulkV2QueryReader bulkReader = null;
        try {
            BulkV2QueryController bulkController = new BulkV2QueryController(_connectionManager);
            // start the bulk load operation steps
            bulkController.startQueryBulk(queryString);
            // waits for Salesforce to finish processing
            BulkManager.waitQuery(bulkController);

            String pageLocator = null;
            Long pageSize = _connectionManager.getOperationProperties().getPageSize();
            // Read Bulk Job results
            bulkReader = new BulkV2QueryReader(_connectionManager, bulkController.getJobId(),
                                               bulkController.getTotalRecordsCount());
            do {
                bulkReader.initQueryResult(pageLocator, pageSize);
                while (bulkReader.hasNext()) {
                    _response.addPartialResult(requestData, OperationStatus.SUCCESS, "200", "success",
                                               bulkReader.getNext());
                }

                pageLocator = bulkReader.getNextPageLocator();
            } while (pageLocator != null);
            _response.finishPartialResult(requestData);
        } finally {
            if (bulkReader != null) {
                bulkReader.close();
            }
        }
    }
}