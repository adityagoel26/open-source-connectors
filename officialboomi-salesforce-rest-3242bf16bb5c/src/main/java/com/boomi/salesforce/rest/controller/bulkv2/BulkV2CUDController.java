// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2;

import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.util.JSONUtils;
import com.boomi.util.IOUtil;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.InputStream;

/**
 * Controller responsible to manage Salesforce Bulk API V2 steps for Create Update Upsert Delete operations.
 */
public class BulkV2CUDController extends BulkV2Controller {

    /**
     * @param connectionManager SFRestConnection instance
     */
    public BulkV2CUDController(SFRestConnection connectionManager) {
        super(connectionManager);
    }

    /**
     * Starts bulk loading steps: Creates a new JOB and store the jobID, Uploads csvData data batch, then marks JOB as
     * finished.
     *
     * @param csvData input csv batch data to be uploaded
     */
    public void startBulk(InputStream csvData, long contentLength) {
        ClassicHttpResponse createJobResponse = _connectionManager.getRequestHandler().createBulkV2Job();
        try {
            // gets the value and closes the stream
            _jobID = JSONUtils.getValueSafely(createJobResponse, "id");

            _connectionManager.getConnectionProperties().logFine("Starting to upload data. Job ID: " + _jobID);
            _connectionManager.getRequestHandler().uploadData(_jobID, csvData, contentLength);

            _connectionManager.getRequestHandler().finishUpload(_jobID);
            _connectionManager.getConnectionProperties().logFine("Finished uploading data.");
        } finally {
            IOUtil.closeQuietly(createJobResponse);
        }
    }

    /**
     * Requests salesforce to check if salesforce finished processing data. If yes saves the total number of records
     * processed and the number of failed records.<br>
     *
     * @return true if Salesforce finished processing the job
     */
    @Override
    public boolean isFinishedProcessing() {
        ClassicHttpResponse statusResponse = _connectionManager.getRequestHandler().getBulkStatus(_jobID);
        try {
            return super.isFinishedProcessing(statusResponse);
        } finally {
            IOUtil.closeQuietly(statusResponse);
        }
    }

    /**
     * If there is successful records, requests salesforce to get the successful records in CSV, returns null otherwise
     */
    public ClassicHttpResponse getCSVSuccessResults() {
        ClassicHttpResponse response = null;
        if (_numberRecordsProcessed != _numberRecordsFailed) {
            response = _connectionManager.getRequestHandler().getBulkSuccessResult(_jobID);
        }
        return response;
    }

    /**
     * If there is failed records, requests salesforce to get the failed records in CSV, returns null otherwise
     */
    public ClassicHttpResponse getCSVFailedResults() {
        ClassicHttpResponse response = null;
        if (_numberRecordsFailed != 0) {
            response = _connectionManager.getRequestHandler().getFailedResult(_jobID);
        }
        return response;
    }
}
