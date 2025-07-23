// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.batch.Batch;
import com.boomi.connector.googlebq.operation.batch.BatchFactory;
import com.boomi.connector.googlebq.resource.TableDataResource;
import com.boomi.connector.util.BaseUpdateOperation;

import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.IOException;
import java.util.logging.Level;

public class GoogleBqStreamingOperation extends BaseUpdateOperation {

    private static final long MAX_BATCH_SIZE_IN_BYTES = 10482760;
    private static final int DEFAULT_BATCH_COUNT = 500;
    private static final int MAX_BATCH_COUNT = 10000;


    public GoogleBqStreamingOperation(GoogleBqOperationConnection conn ) {
        super(conn);
    }

    @Override
    public GoogleBqOperationConnection getConnection() {
        return (GoogleBqOperationConnection) super.getConnection();
    }

    @Override
    protected void executeUpdate( UpdateRequest request, OperationResponse operationResponse )
    {
        BatchFactory batchFactory;
        try {
            PropertyMap operationProperties = getContext().getOperationProperties();
            Long batchCount = operationProperties.getLongProperty(GoogleBqConstants.PROP_BATCH_COUNT);

            int count = useDefault(batchCount) ? DEFAULT_BATCH_COUNT : batchCount.intValue();
            if (count > MAX_BATCH_COUNT) {
                throw new ConnectorException("Batch Count exceeds maximum allowable count of 10000");
            }

            String templateSuffix = operationProperties.getProperty(GoogleBqConstants.PROP_TEMPLATE_SUFFIX);
            batchFactory = new BatchFactory(request, templateSuffix, count, MAX_BATCH_SIZE_IN_BYTES, operationResponse);
        } catch( ConnectorException e ) {
            ResponseUtil.addExceptionFailures(operationResponse, request, e);
            return;
        }

        processBatches(batchFactory, operationResponse);
    }

    private void processBatches( BatchFactory batchFactory, OperationResponse opResponse )
    {
        TableDataResource tableDataResource = new TableDataResource(getConnection());

        for (Batch batch : batchFactory) {
            try {
                Response response = tableDataResource.insertAll(batch);
                Status status = response.getStatus();
                if (status.isSuccess()) {
                    addResult(tableDataResource, batch, response);
                }
                else {
                    OperationStatus opStatus = (status.isServerError() || status.isConnectorError()) ?
                            OperationStatus.FAILURE :
                            OperationStatus.APPLICATION_ERROR;
                    batch.addCombinedErrorResult(response, null, opStatus);
                    if (Status.SERVER_ERROR_SERVICE_UNAVAILABLE.equals(response.getStatus())) {
                        // If not all batches were processed, it's because there was a service unavailable error
                        batchFactory.markRemainingDocumentsAsFailed(opResponse, response);
                        return;
                    }
                }
            }
            catch (Exception e) {
                batch.addApplicationErrors(e);
            }
        }
    }

    private static boolean useDefault( Long batchCount )
    {
        return batchCount == null || batchCount <= 0;
    }

    private static void addResult( TableDataResource resource, Batch batch, Response response ) throws IOException
    {
        try {
            batch.addResult(resource.createIndexToErrorMap(response, batch.getBatchCount()));
        } catch( IndexOutOfBoundsException e ) {
            batch.getProcessLogger().log(Level.SEVERE, e.getMessage(), e);
            batch.addCombinedErrorResult(response, e.getMessage(), OperationStatus.APPLICATION_ERROR);
        }
    }
}
