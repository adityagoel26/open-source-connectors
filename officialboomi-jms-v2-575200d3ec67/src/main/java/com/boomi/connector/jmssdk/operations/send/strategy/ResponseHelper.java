// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to facilitate adding to a {@link OperationResponse} the result of Sending JMS Messages.
 */
final class ResponseHelper {

    private static final Logger LOG = LogUtil.getLogger(ResponseHelper.class);

    private static final ObjectMapper OBJECT_MAPPER = JSONUtil.getDefaultObjectMapper();
    private static final String NO_CODE = StringUtil.EMPTY_STRING;
    private static final String TRANSACTION_ERROR_MESSAGE =
            "Some of the messages from the transaction could not be sent";

    private ResponseHelper() {
    }

    /**
     * Add a success result to the provided {@link OperationResponse} for the document associated to the given
     * {@link SendMessageMetadata}.
     *
     * @param sendResult containing metadata about the sent message.
     * @param response   the operation response where the result will be added.
     */
    static void addSuccess(SendResult sendResult, OperationResponse response) {
        TrackedData document = sendResult.getDocument();
        Payload payload = sendResult.getMessageMetadata().toPayload();
        ResponseUtil.addSuccess(response, document, NO_CODE, payload);
    }

    /**
     * Add a success result to the provided {@link OperationResponse} for every document associated to the given
     * collection of {@link SendResult}s.
     *
     * @param processedResults the results associated to the sent documents.
     * @param response         the operation response where the results will be added.
     */
    static void addSuccesses(Iterable<SendResult> processedResults, OperationResponse response) {
        for (SendResult result : processedResults) {
            addSuccess(result, response);
        }
    }

    /**
     * Add an error result to the provided {@link OperationResponse} for the given document.
     *
     * @param document     to be added as an error.
     * @param errorMessage associated to the error.
     * @param response     the operation response where the result will be added.
     */
    static void addError(TrackedData document, String errorMessage, OperationResponse response) {
        response.addResult(document, OperationStatus.APPLICATION_ERROR, NO_CODE, errorMessage, null);
    }

    /**
     * Add a Combined Application Error result for the documents provided to the given {@link OperationResponse}.
     * <p>
     * A custom JSON payload is generated and added to the combined result. The payload includes the error message
     * extracted from the {@code transactionResult} and the index of the first error document. If the custom payload
     * cannot be generated, a {@code null} payload is used instead.
     *
     * @param batch             the documents that were not processed.
     * @param processedSize     the quantity of documents processed.
     * @param transactionResult the result of the whole transaction.
     * @param response          the operation response where the results will be added.
     */
    static void addErrors(Collection<ObjectData> batch, int processedSize, SendResult transactionResult,
            OperationResponse response) {
        boolean arePendingDocuments = (batch.size() - processedSize) > 0;
        String errorMessage = transactionResult.getErrorMessage();

        Payload payload = createErrorPayload(processedSize, arePendingDocuments, errorMessage);

        response.addCombinedResult(batch, OperationStatus.APPLICATION_ERROR, NO_CODE, TRANSACTION_ERROR_MESSAGE,
                payload);
    }

    private static Payload createErrorPayload(int resultCount, boolean arePendingDocuments, String errorMessage) {
        CollectionUtil.MapBuilder<String, Object> payloadBuilder = CollectionUtil.<String, Object>mapBuilder().put(
                "errorMessage", errorMessage);

        if (arePendingDocuments) {
            // 0-based index
            int errorDocumentIndex = resultCount - 1;
            payloadBuilder.put("errorDocumentIndex", errorDocumentIndex);
        }

        Payload payload = null;
        try {
            payload = PayloadUtil.toPayload(OBJECT_MAPPER.writeValueAsString(payloadBuilder.finishImmutable()));
        } catch (JsonProcessingException e) {
            LOG.log(Level.WARNING, "cannot generate error payload", e);
        }
        return payload;
    }
}