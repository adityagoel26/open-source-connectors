// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSSender;

/**
 * Concrete implementation of {@link SendStrategy} for sending standalone messages
 */
class SimpleSendStrategy extends SendStrategy {

    SimpleSendStrategy(GenericJndiBaseAdapter adapter, JMSSender sender, long documentSizeThreshold) {
        super(adapter, sender, documentSizeThreshold);
    }

    @Override
    public void send(Iterable<ObjectData> documents, OperationResponse response, String objectTypeId) {
        for (ObjectData document : documents) {
            SendResult sendResult = send(document, objectTypeId);
            if (sendResult.isSuccess()) {
                ResponseHelper.addSuccess(sendResult, response);
            } else {
                String errorMessage = sendResult.getErrorMessage();
                ResponseHelper.addError(document, errorMessage, response);
            }
        }
    }
}
