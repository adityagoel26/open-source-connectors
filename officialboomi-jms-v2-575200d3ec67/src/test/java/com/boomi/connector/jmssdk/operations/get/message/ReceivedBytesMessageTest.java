// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.api.Payload;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.doubles.BytesMessageDouble;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

public class ReceivedBytesMessageTest {

    @Test
    public void toPayloadTest() throws IOException {
        byte[] expectedBytes = ("the bytes message payload " + UUID.randomUUID()).getBytes(StringUtil.UTF8_CHARSET);
        String destination = "the destination";
        Message bytesMessage = new BytesMessageDouble(destination, expectedBytes);
        ReceivedMessage receivedMessage = ReceivedMessage.wrapMessage(bytesMessage, DestinationType.BYTE_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class), Mockito.mock(TargetDestination.class));
        SimplePayloadMetadata metadata = new SimplePayloadMetadata();

        Payload payload = receivedMessage.toPayload(metadata);

        InputStream stream = payload.readFrom();
        byte[] actualBytes = new byte[stream.available()];
        StreamUtil.readFully(stream, actualBytes);
        assertArrayEquals("the payload content should match the message bytes", expectedBytes, actualBytes);

        Map<String, String> trackedProps = metadata.getTrackedProps();
        assertThat(trackedProps.get("destination"), is(destination));
        assertThat(trackedProps.get("message_type"), is("BYTE_MESSAGE"));
    }
}
