// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.message;

import oracle.jms.AdtMessage;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.OracleAQAdapter;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.util.StreamUtil;

import org.w3c.dom.Document;

import javax.jms.Message;

import java.io.InputStream;

public class ReceivedADTMessage extends ReceivedMessage {

    private final TargetDestination _targetDestination;
    private final OracleAQAdapter _adapter;

    ReceivedADTMessage(Message message, OracleAQAdapter adapter, TargetDestination targetDestination) {
        super(message);
        _targetDestination = targetDestination;
        _adapter = adapter;
    }

    @Override
    DestinationType getDestinationType() {
        return DestinationType.ADT_MESSAGE;
    }

    @Override
    InputStream getMessagePayload() {
        return StreamUtil.EMPTY_STREAM;
    }

    @Override
    public Payload toPayload(PayloadMetadata metadata) {
        Document adtMessageDocument = _adapter.createAdtMessageDocument(_targetDestination, (AdtMessage) _message);
        fillMetadata(metadata);
        return PayloadUtil.toPayload(adtMessageDocument, metadata);
    }
}
