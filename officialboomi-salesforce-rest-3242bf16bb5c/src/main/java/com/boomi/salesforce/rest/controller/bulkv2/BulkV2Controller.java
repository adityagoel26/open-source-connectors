// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.controller.bulkv2;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.util.JSONUtils;
import com.boomi.util.StringUtil;
import org.apache.hc.core5.http.ClassicHttpResponse;

import java.util.Arrays;
import java.util.Map;

/**
 * Controller responsible to manage Salesforce Bulk API V2 steps.
 */
public abstract class BulkV2Controller {
    protected String _jobID;
    protected int _numberRecordsProcessed;
    protected int _numberRecordsFailed;

    protected SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public BulkV2Controller(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    /**
     * Requests salesforce to check if salesforce finished processing data.
     *
     * @return true if finished, false otherwise
     */public abstract boolean isFinishedProcessing();

    /**
     * Requests salesforce to check if salesforce finished processing data. If yes gets the total number of records
     * processed and the number of failed records.
     *
     * @return true if ready to request for bulk results
     */
    protected boolean isFinishedProcessing(ClassicHttpResponse statusResponse) {
        // gets node, then closes the stream
        Map<String, String> keyValues = JSONUtils.getValuesMapSafely(statusResponse,
                                                                     Arrays.asList("state", "errorMessage",
                                                                                   "numberRecordsProcessed",
                                                                                   "numberRecordsFailed"));
        String state = keyValues.get("state");
        if ("JobComplete".contentEquals(state)) {
            saveRecordsCount(keyValues);
            return true;
        } else if ("Failed".contentEquals(state)) {
            String error = keyValues.get("errorMessage");
            saveRecordsCount(keyValues);
            // failed to build the job
            if (StringUtil.isNotBlank(error)) {
                throw new ConnectorException("[Salesforce job failed] " + error);
            }
            return true;
        }
        return false;
    }

    /**
     * Gets the number of records processed and failed from the bulk status json response and logs it
     *
     * @param keys contains Job Status JSON response
     */
    private void saveRecordsCount(Map<String, String> keys) {
        String total = keys.get("numberRecordsProcessed");
        String failed = keys.get("numberRecordsFailed");
        String logMessage = "";
        if (StringUtil.isNotBlank(total)) {
            _numberRecordsProcessed = Integer.parseInt(total);
            logMessage = "Number of Records Processed: " + _numberRecordsProcessed + ".";
        }
        if (StringUtil.isNotBlank(failed)) {
            _numberRecordsFailed = Integer.parseInt(failed);
            logMessage += " Number of Records Failed: " + _numberRecordsFailed;
        }
        _connectionManager.getConnectionProperties().logInfo(logMessage);
    }

    /**
     * @return the number of failed records in the job
     */
    public int getFailedRecordsCount() {
        return _numberRecordsFailed;
    }

    /**
     * @return the jobID
     */
    public String getJobId() {
        return _jobID;
    }

    /**
     * @return the total number of records processed in the job
     */
    public int getTotalRecordsCount() {
        return _numberRecordsProcessed;
    }

}
