// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2.reader;

import com.boomi.salesforce.rest.SFRestConnection;

/**
 * Uses CsvReader to read and split the output to multiple payloads.<br> Limited to 1,000,000 characters per record
 */
public class BulkV2CUDReader extends BulkV2Reader {

    /**
     * @param connectionManager      SFRestConnection instance
     * @param jobID                  ID of the job to be read
     * @param numberRecordsProcessed number of records Processed in the job
     * @param numberRecordsFailed    number of records failed in the job
     */
    public BulkV2CUDReader(SFRestConnection connectionManager, String jobID, int numberRecordsProcessed,
                           int numberRecordsFailed) {
        super(connectionManager, jobID, numberRecordsProcessed, numberRecordsFailed);
    }

    /**
     * Returns a list of documents that contain all the bulk successfully processed
     *
     * @return true if there is successful results and ready to be read
     */
    public boolean initSuccessResultStream() {
        if (_numberRecordsProcessed != _numberRecordsFailed) {
            _response = _connectionManager.getRequestHandler().getBulkSuccessResult(_jobID);
            initReader();
            return true;
        }
        return false;
    }

    /**
     * Returns a list of documents that contain all the bulk failed result documents
     *
     * @return true if there is successful results and ready to be read
     */
    public boolean initFailedResultStream() {
        if (_numberRecordsFailed != 0) {
            _response = _connectionManager.getRequestHandler().getFailedResult(_jobID);
            initReader();
            return true;
        }
        return false;
    }
}
