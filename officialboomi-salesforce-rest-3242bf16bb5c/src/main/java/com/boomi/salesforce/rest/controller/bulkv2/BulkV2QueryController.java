// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2;

import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.util.JSONUtils;
import com.boomi.util.IOUtil;
import org.apache.hc.core5.http.ClassicHttpResponse;

/**
 * Controller responsible to manage Salesforce Bulk API V2 steps for Query operations.
 */
public class BulkV2QueryController extends BulkV2Controller {

    /**
     * @param connectionManager SFRestConnection instance
     */
    public BulkV2QueryController(SFRestConnection connectionManager) {
        super(connectionManager);
    }

    /**
     * Starts bulk unloading, creates a new JOB with the SOQL query string and store the jobID.
     *
     * @param queryString the SOQL query
     */
    public void startQueryBulk(String queryString) {
        boolean logSOQL = _connectionManager.getOperationProperties().getLogSOQL();
        if (logSOQL) {
            _connectionManager.getConnectionProperties().logInfo("Executing SOQL: " + queryString);
        }

        ClassicHttpResponse createJobResponse = _connectionManager.getRequestHandler()
                                                                  .createBulkV2QueryJob(queryString);
        try {
            // gets value and closes the stream
            _jobID = JSONUtils.getValueSafely(createJobResponse, "id");
            _connectionManager.getConnectionProperties().logFine("Created bulk query. Job ID: " + _jobID);
        } finally {
            IOUtil.closeQuietly(createJobResponse);
        }
    }

    /**
     * Requests Salesforce to check if finished processing data. If yes gets the total number of records processed and
     * the number of failed records.
     *
     * @return true if Salesforce finished processing the job
     */
    @Override
    public boolean isFinishedProcessing() {
        ClassicHttpResponse statusResponse = _connectionManager.getRequestHandler().getBulkQueryStatus(_jobID);
        try {
            return super.isFinishedProcessing(statusResponse);
        } finally {
            IOUtil.closeQuietly(statusResponse);
        }
    }

    /**
     * Requests salesforce to get the successful records in CSV
     *
     * @param pageSize    Target number of documents received in this request
     * @param pageLocator Locator to the current page result
     */
    public ClassicHttpResponse getCSVResults(String pageLocator, Long pageSize) {
        return _connectionManager.getRequestHandler().getBulkQueryResult(_jobID, pageLocator, pageSize);
    }
}
