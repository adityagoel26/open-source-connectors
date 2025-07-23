// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2.xml;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.TrackedData;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.controller.bulkv2.writer.BulkV2CUDWriter;
import com.boomi.util.IOUtil;

import java.io.InputStream;
import java.util.List;

/**
 * Combines a batch of XML inputs in one CSV file
 */
public class SFBulkV2Combiner {
    private final OperationResponse _response;
    private final SFRestConnection _connectionManager;

    /**
     * @param connectionManager SFRestConnection instance
     */
    public SFBulkV2Combiner(SFRestConnection connectionManager, OperationResponse response) {
        _response = response;
        _connectionManager = connectionManager;
    }

    /**
     * Writes the bulk batch in CSV of the given sizeLimited list of XML inputs<br>
     *
     * @param requestBatch the XML documents to be uploaded in this batch
     * @param isUpdate     if true will execute Create/Update bulk. Bulk Delete otherwise
     * @return Pair of InputStream the written CSV batch data and it size in Bytes
     */
    private Pair<InputStream, Long> writeUpdateBatch(List<TrackedData> requestBatch, boolean isUpdate) {
        InputStream bulkBatch;
        long contentLength = -1;
        BulkV2CUDWriter bulkWriter = new BulkV2CUDWriter(_connectionManager);
        try {
            bulkWriter.init();

            int index = 0;
            while (index < requestBatch.size()) {
                TrackedData batch = requestBatch.get(index);
                InputStream inputData = null;
                try {
                    try {
                        if (isUpdate) {
                            inputData = ((ObjectData) batch).getData();
                            bulkWriter.receive(inputData);
                        } else {
                            String id = ((ObjectIdData) batch).getObjectId();
                            bulkWriter.receiveDelete(id);
                        }
                    } catch (Exception e) {
                        _response.addErrorResult(batch, OperationStatus.APPLICATION_ERROR, "", "Exception Occurred", e);
                        requestBatch.remove(index);
                        --index;
                    }
                    index++;
                } finally {
                    IOUtil.closeQuietly(inputData);
                }
            }

            bulkBatch = bulkWriter.getInputStream();
            contentLength = bulkWriter.getContentLength();
        } finally {
            IOUtil.closeQuietly(bulkWriter);
        }

        if (contentLength > 0) {
            return new Pair<>(bulkBatch, contentLength);
        }
        return null;
    }

    /**
     * Writes the bulk batch in CSV of the given sizeLimited list of XML inputs<br>
     *
     * @param requestBatch the XML documents to be uploaded in this batch
     * @return Pair of InputStream the written CSV batch data and it size in Bytes
     */
    public Pair<InputStream, Long> writeUpdateBatch(List<TrackedData> requestBatch) {
        return writeUpdateBatch(requestBatch, true);
    }

    /**
     * Writes the bulk batch in CSV of the given sizeLimited list of XML inputs<br>
     *
     * @param requestBatch the XML documents to be uploaded in this batch
     * @return Pair of InputStream the written CSV batch data and it size in Bytes
     */
    public Pair<InputStream, Long> writeDeleteBatch(List<TrackedData> requestBatch) {
        return writeUpdateBatch(requestBatch, false);
    }
}
