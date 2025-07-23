// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.doubles.BytesMessageDouble;
import com.boomi.connector.testutil.doubles.MapMessageDouble;

import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.util.Map;
import java.util.Vector;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ReceivedMessageTest {

    @Test
    public void wrapMessageTest() {
        Message mapMessage = new MapMessageDouble();
        Message bytesMessage = new BytesMessageDouble();
        Message textMessage = mock(TextMessage.class);

        ReceivedMessage receivedMapMessage = ReceivedMessage.wrapMessage(mapMessage, DestinationType.MAP_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class), Mockito.mock(TargetDestination.class));
        ReceivedMessage receivedBytesMessage = ReceivedMessage.wrapMessage(bytesMessage, DestinationType.BYTE_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class), Mockito.mock(TargetDestination.class));
        ReceivedMessage receivedTextMessage = ReceivedMessage.wrapMessage(textMessage, DestinationType.TEXT_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class), Mockito.mock(TargetDestination.class));

        assertThat(receivedMapMessage, is(instanceOf(ReceivedMapMessage.class)));
        assertTrue("it should contain a message", receivedMapMessage.hasMessage());

        assertThat(receivedBytesMessage, is(instanceOf(ReceivedBytesMessage.class)));
        assertTrue("it should contain a message", receivedBytesMessage.hasMessage());

        assertThat(receivedTextMessage, is(instanceOf(ReceivedTextMessage.class)));
        assertTrue("it should contain a message", receivedTextMessage.hasMessage());
    }

    @Test
    public void emptyMessageTest() {
        ReceivedMessage receivedMessage = ReceivedMessage.emptyMessage();

        assertFalse("it shouldn't contain a message", receivedMessage.hasMessage());
    }

    @Test
    public void fillMetadataTest_noDestinationInfoInMessage() throws JMSException {
        Message mockedMsg = mock(Message.class);
        // Setup test data
        final String jmsCorrelationId = "someJmsCorrelationId";
        doReturn(jmsCorrelationId).when(mockedMsg).getJMSCorrelationID();

        final String jmsMsgId = "someJmsMessageId";
        doReturn(jmsMsgId).when(mockedMsg).getJMSMessageID();

        doReturn(null).when(mockedMsg).getJMSDestination();

        final int jmsPriority = 2;
        doReturn(jmsPriority).when(mockedMsg).getJMSPriority();

        final long jmsExpiration = 100000;
        doReturn(jmsExpiration).when(mockedMsg).getJMSExpiration();

        Destination mockedReplyToDest = mock(Destination.class);
        doReturn(mockedReplyToDest).when(mockedMsg).getJMSReplyTo();
        final String replyTo = "replyToSomeDestination";
        doReturn(replyTo).when(mockedReplyToDest).toString();

        final boolean redeliveredMsg = true;
        doReturn(redeliveredMsg).when(mockedMsg).getJMSRedelivered();

        Vector<String> propNames = new Vector<>();
        final String prop1Name = "prop1";
        final String prop2Name = "prop2";
        propNames.add(prop1Name);
        propNames.add(prop2Name);
        doReturn(propNames.elements()).when(mockedMsg).getPropertyNames();
        final int prop1Val = 11;
        final long prop2Val = 555555;
        doReturn(prop1Val).when(mockedMsg).getObjectProperty(prop1Name);
        doReturn(prop2Val).when(mockedMsg).getObjectProperty(prop2Name);

        ReceivedMessage receivedMessage = ReceivedMessage.wrapMessage(mockedMsg, DestinationType.TEXT_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class), Mockito.mock(TargetDestination.class));
        SimplePayloadMetadata metadata = new SimplePayloadMetadata();
        receivedMessage.fillMetadata(metadata);

        // Verify destination value first.
        assertNull("Destination instance should be null as it is not set in instance of Message",
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_DESTINATION));

        // Verify rest of the parameters are unaffected by destination value.
        assertEquals("Correlation id must match the one specified in the setup step.", jmsCorrelationId,
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_CORRELATION_ID));
        assertEquals("Message id must match the one specified in the setup step.", jmsMsgId,
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_MESSAGE_ID));
        assertEquals("Message type must match the instance of ReceivedMessage.", DestinationType.TEXT_MESSAGE.name(),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_MESSAGE_TYPE));
        assertEquals("Message priority must match the one specified in the setup step.", String.valueOf(jmsPriority),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_PRIORITY));
        assertEquals("Expiration time must match the one specified in the setup step.", String.valueOf(jmsExpiration),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_EXPIRATION_TIME));
        assertEquals("Destination name to reply must match the one specified in the setup step.", replyTo,
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_REPLY_TO));
        assertEquals("Message class name must match mocked message class name.", mockedMsg.getClass().toString(),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_MESSAGE_CLASS));
        assertEquals("Redelivered message value must match the one specified in the setup step.",
                String.valueOf(redeliveredMsg),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_REDELIVERED));
        Map<String, String> propertyGroup = metadata.getTrackedGroups().get(JMSConstants.TRACKED_PROPERTY_GROUP);
        assertEquals("Number of properties in property group must match the one set in the setup step.", 2,
                propertyGroup.size());
        assertEquals("Value of property group prop1 must match the one specified in the setup step.",
                String.valueOf(prop1Val), propertyGroup.get(prop1Name));
        assertEquals("Value of property group prop2 must match the one specified in the setup step.",
                String.valueOf(prop2Val), propertyGroup.get(prop2Name));
    }

    @Test
    public void fillMetadataTest_withDestinationInfoInMessage() throws JMSException {
        Message mockedMsg = mock(Message.class);
        // Setup test data
        final String jmsCorrelationId = "someJmsCorrelationId";
        doReturn(jmsCorrelationId).when(mockedMsg).getJMSCorrelationID();

        final String jmsMsgId = "someJmsMessageId";
        doReturn(jmsMsgId).when(mockedMsg).getJMSMessageID();

        Destination mockedMsgDestination = mock(Destination.class);
        doReturn(mockedMsgDestination).when(mockedMsg).getJMSDestination();
        final String msgDestinationName = "someMessageDestination";
        doReturn(msgDestinationName).when(mockedMsgDestination).toString();

        final int jmsPriority = 2;
        doReturn(jmsPriority).when(mockedMsg).getJMSPriority();

        final long jmsExpiration = 100000;
        doReturn(jmsExpiration).when(mockedMsg).getJMSExpiration();

        Destination mockedReplyToDest = mock(Destination.class);
        doReturn(mockedReplyToDest).when(mockedMsg).getJMSReplyTo();
        final String replyTo = "replyToSomeDestination";
        doReturn(replyTo).when(mockedReplyToDest).toString();

        final boolean redeliveredMsg = true;
        doReturn(redeliveredMsg).when(mockedMsg).getJMSRedelivered();

        Vector<String> propNames = new Vector<>();
        final String prop1Name = "prop1";
        final String prop2Name = "prop2";
        propNames.add(prop1Name);
        propNames.add(prop2Name);
        doReturn(propNames.elements()).when(mockedMsg).getPropertyNames();
        final int prop1Val = 11;
        final long prop2Val = 555555;
        doReturn(prop1Val).when(mockedMsg).getObjectProperty(prop1Name);
        doReturn(prop2Val).when(mockedMsg).getObjectProperty(prop2Name);

        ReceivedMessage receivedMessage = ReceivedMessage.wrapMessage(mockedMsg, DestinationType.TEXT_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class), Mockito.mock(TargetDestination.class));
        SimplePayloadMetadata metadata = new SimplePayloadMetadata();
        receivedMessage.fillMetadata(metadata);

        // Verify destination value first.
        assertEquals("Message destination name must match the one specified in the setup step.", msgDestinationName,
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_DESTINATION));

        // Verify rest of the parameters are unaffected by destination value.
        assertEquals("Correlation id must match the one specified in the setup step.", jmsCorrelationId,
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_CORRELATION_ID));
        assertEquals("Message id must match the one specified in the setup step.", jmsMsgId,
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_MESSAGE_ID));
        assertEquals("Message type must match the instance of ReceivedMessage.", DestinationType.TEXT_MESSAGE.name(),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_MESSAGE_TYPE));
        assertEquals("Message priority must match the one specified in the setup step.", String.valueOf(jmsPriority),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_PRIORITY));
        assertEquals("Expiration time must match the one specified in the setup step.", String.valueOf(jmsExpiration),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_EXPIRATION_TIME));
        assertEquals("Destination name to reply must match the one specified in the setup step.", replyTo,
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_REPLY_TO));
        assertEquals("Message class name must match mocked message class name.", mockedMsg.getClass().toString(),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_MESSAGE_CLASS));
        assertEquals("Redelivered message value must match the one specified in the setup step.",
                String.valueOf(redeliveredMsg),
                metadata.getTrackedProps().get(JMSConstants.TRACKED_PROPERTY_REDELIVERED));
        Map<String, String> propertyGroup = metadata.getTrackedGroups().get(JMSConstants.TRACKED_PROPERTY_GROUP);
        assertEquals("Number of properties in property group must match the one set in the setup step.", 2,
                propertyGroup.size());
        assertEquals("Value of property group prop1 must match the one specified in the setup step.",
                String.valueOf(prop1Val), propertyGroup.get(prop1Name));
        assertEquals("Value of property group prop2 must match the one specified in the setup step.",
                String.valueOf(prop2Val), propertyGroup.get(prop2Name));
    }
}
