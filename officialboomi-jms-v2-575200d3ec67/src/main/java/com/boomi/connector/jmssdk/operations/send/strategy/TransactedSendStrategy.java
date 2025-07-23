// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSSender;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Concrete implementation of {@link SendStrategy} for sending batches of messages within transactions
 */
class TransactedSendStrategy extends SendStrategy {

    private static final Logger LOG = LogUtil.getLogger(TransactedSendStrategy.class);

    private final int _transactionBatchSize;

    TransactedSendStrategy(GenericJndiBaseAdapter adapter, JMSSender sender, long documentSizeThreshold,
            int transactionBatchSize) {
        super(adapter, sender, documentSizeThreshold);

        _transactionBatchSize = transactionBatchSize;
    }

    @Override
    public void send(Iterable<ObjectData> documents, OperationResponse response, String objectTypeId) {
        Iterable<List<ObjectData>> batches = CollectionUtil.pageIterable(documents, _transactionBatchSize);
        for (List<ObjectData> batch : batches) {
            processBatch(batch, response, objectTypeId);
        }
    }

    private void processBatch(Collection<ObjectData> batch, OperationResponse response, String objectTypeId) {
        Collection<SendResult> processedResults = new ArrayList<>();

        for (ObjectData document : batch) {
            SendResult sendResult = send(document, objectTypeId);
            processedResults.add(sendResult);
            if (!sendResult.isSuccess()) {
                // transaction failed, rolling back and terminate batch processing
                rollback(sendResult, batch, processedResults.size(), response);
                return;
            }
        }

        commit(processedResults, response);
    }

    private void commit(Collection<SendResult> processedResults, OperationResponse response) {
        try {
            _sender.commit();
            ResponseHelper.addSuccesses(processedResults, response);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "error committing transaction", e);
            rollback(SendResult.error(null, e),
                    processedResults.stream().map(sendResult -> (ObjectData) sendResult.getDocument())
                            .collect(Collectors.toList()), processedResults.size(), response);
        }
    }

    private void rollback(SendResult transactionResult, Collection<ObjectData> batch, int processedSize,
            OperationResponse response) {
        _sender.rollback();
        ResponseHelper.addErrors(batch, processedSize, transactionResult, response);
    }
}