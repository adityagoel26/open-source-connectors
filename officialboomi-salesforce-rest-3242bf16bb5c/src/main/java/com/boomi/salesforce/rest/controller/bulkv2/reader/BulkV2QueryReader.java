// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2.reader;

import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;

/**
 * Uses CsvReader to read and split the output to multiple payloads.<br> Limited to 1,000,000 characters per record
 */
public class BulkV2QueryReader extends BulkV2Reader {

    /**
     * @param connectionManager      SFRestConnection instance
     * @param jobID                  ID of the job to be read
     * @param numberRecordsProcessed number of records Processed in the job
     */
    public BulkV2QueryReader(SFRestConnection connectionManager, String jobID, int numberRecordsProcessed) {
        super(connectionManager, jobID, numberRecordsProcessed, 0);
    }

    /**
     * Requests salesforce for the CSV containing the query bulk result<br> initialize the CSV reader with the response
     * stream
     *
     * @param pageLocator locator to the current Page for QUERY. Can be null
     * @param pageSize    number of documents in the returned request. Can be null
     */
    public void initQueryResult(String pageLocator, Long pageSize) {
        _response = _connectionManager.getRequestHandler().getBulkQueryResult(_jobID, pageLocator, pageSize);
        initReader();
    }

    public String getNextPageLocator() {
        return SalesforceResponseUtil.getQueryPageLocatorQuietly(_response);
    }
}
