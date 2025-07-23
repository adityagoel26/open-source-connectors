// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.api.Payload;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.testutil.NoLoggingTest;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.doubles.TextMessageDouble;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.jms.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class ReceivedTextMessageTest extends NoLoggingTest {

    private final String _textValue;
    private final String _expectedMessageType;

    public ReceivedTextMessageTest(String textValue, String expectedMessageType, String description) {
        _textValue = textValue;
        _expectedMessageType = expectedMessageType;
    }

    @Parameterized.Parameters(name = "{2}")
    public static Iterable<String[]> testCases() {
        Collection<String[]> testCases = new ArrayList<>();
        {
            String description = "Payload containing XML document";
            String textValue =  "<xml>this is the xml payload for the text message " + UUID.randomUUID() + "</xml>";
            String expectedMessageType = "TEXT_MESSAGE_XML";

            testCases.add(new String[] { textValue, expectedMessageType, description });
        }

        {
            String description = "Payload containing plain text";
            String textValue =  "this is the payload for the text message " + UUID.randomUUID();
            String expectedMessageType = "TEXT_MESSAGE";

            testCases.add(new String[] { textValue, expectedMessageType, description });
        }

        return testCases;
    }

    @Test
    public void toPayloadTest() throws IOException {
        String destination = "the destination";
        Message textMessage = new TextMessageDouble(destination, _textValue);
        ReceivedMessage receivedMessage = ReceivedMessage.wrapMessage(textMessage, DestinationType.TEXT_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class),Mockito.mock(TargetDestination.class));
        SimplePayloadMetadata metadata = new SimplePayloadMetadata();

        Payload payload = receivedMessage.toPayload(metadata);

        InputStream stream = payload.readFrom();
        String actualValue = StreamUtil.toString(stream, StringUtil.UTF8_CHARSET);

        assertThat(actualValue, is(_textValue));

        Map<String, String> trackedProps = metadata.getTrackedProps();
        assertThat(trackedProps.get("destination"), is(destination));
        assertThat(trackedProps.get("message_type"), is(_expectedMessageType));
    }
}