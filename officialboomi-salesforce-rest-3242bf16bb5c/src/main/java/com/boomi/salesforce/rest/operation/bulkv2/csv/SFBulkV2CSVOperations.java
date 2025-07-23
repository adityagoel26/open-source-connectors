// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2.csv;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2CUDController;
import com.boomi.salesforce.rest.controller.bulkv2.BulkV2QueryController;
import com.boomi.salesforce.rest.controller.operation.QueryController;
import com.boomi.salesforce.rest.operation.bulkv2.BulkManager;
import com.boomi.salesforce.rest.util.SalesforceResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Send each CSV input document as a single batch for bulk load operation.<br> Does not require input size validation
 */
public class SFBulkV2CSVOperations {

    private static final String BULK_ERROR_FORMAT = "Salesforce failed to %s records";

    private static final Logger LOG = LogUtil.getLogger(SFBulkV2CSVOperations.class);

    private final SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public SFBulkV2CSVOperations(SFRestConnection connectionManager) {
        _connectionManager = connectionManager;
    }

    /**
     * Creates bulk job for each CSV input
     *
     * @param request  UpdateRequest object
     * @param response OperationResponse object
     */
    public void executeCUD(UpdateRequest request, OperationResponse response) {
        BulkV2CUDController bulkController = new BulkV2CUDController(_connectionManager);
        String operationName = _connectionManager.getOperationProperties().getOperationBoomiName();
        for (ObjectData input : request) {
            _connectionManager.getOperationProperties().initDynamicProperties(input);
            InputStream inputData = input.getData();
            try {
                long contentLength = -1;
                try {
                    contentLength = input.getDataSize();
                } catch (Exception e) {
                    LOG.log(Level.INFO, e, () -> "an error happened obtaining the object data size: " + e.getMessage());
                }
                bulkController.startBulk(inputData, contentLength);

                BulkManager.waitCUD(bulkController);

                ClassicHttpResponse salesforceResponse = bulkController.getCSVSuccessResults();
                try {
                    if (salesforceResponse != null) {
                        response.addPartialResult(input, OperationStatus.SUCCESS, "200", null,
                                SalesforceResponseUtil.toPayload(salesforceResponse));
                    }
                } finally {
                    IOUtil.closeQuietly(salesforceResponse);
                }
                salesforceResponse = bulkController.getCSVFailedResults();
                try {
                    if (salesforceResponse != null) {
                        String errorMessage = String.format(BULK_ERROR_FORMAT, bulkController.getFailedRecordsCount());
                        response.addPartialResult(input, OperationStatus.APPLICATION_ERROR, "", errorMessage,
                                SalesforceResponseUtil.toPayload(salesforceResponse));
                    }
                } finally {
                    IOUtil.closeQuietly(salesforceResponse);
                }
            } catch (ConnectorException e) {
                String errorMessage = String.format(BULK_ERROR_FORMAT, operationName);
                response.addErrorResult(input, OperationStatus.FAILURE, "", errorMessage, e);
            } finally {
                response.finishPartialResult(input);
                IOUtil.closeQuietly(inputData);
            }
        }
    }

    /**
     * Reads custom SOQL string input document and create bulk query CSV
     *
     * @param request  UpdateRequest object
     * @param response OperationResponse object
     */
    public void executeQuery(UpdateRequest request, OperationResponse response) {
        BulkV2QueryController bulkController = new BulkV2QueryController(_connectionManager);
        String operationName = _connectionManager.getOperationProperties().getOperationBoomiName();
        QueryController soqlController = new QueryController(_connectionManager);
        for (ObjectData input : request) {
            try {
                String queryString = soqlController.readSOQLInput(input);
                bulkController.startQueryBulk(queryString);

                BulkManager.waitQuery(bulkController);

                Long pageSize = _connectionManager.getOperationProperties().getPageSize();
                String pageLocator = null;
                do {
                    ClassicHttpResponse salesforceResponse = bulkController.getCSVResults(pageLocator, pageSize);
                    try {
                        response.addPartialResult(input, OperationStatus.SUCCESS, "200", null,
                                SalesforceResponseUtil.toPayload(salesforceResponse));

                        pageLocator = SalesforceResponseUtil.getQueryPageLocatorQuietly(salesforceResponse);
                    } finally {
                        IOUtil.closeQuietly(salesforceResponse);
                    }
                } while (pageLocator != null);

                response.finishPartialResult(input);
            } catch (Exception e) {
                response.addErrorResult(input, OperationStatus.FAILURE, "",
                        "Salesforce failed to " + operationName + " records", e);
            }
        }
    }
}
