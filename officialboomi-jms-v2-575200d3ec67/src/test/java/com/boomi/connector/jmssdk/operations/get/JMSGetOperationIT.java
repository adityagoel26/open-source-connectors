// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.jmssdk.JMSTestContext;
import com.boomi.connector.jmssdk.client.ClientTestUtils;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.CollectionUtil;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.Message;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Ignore("Configure JMSTestContext with valid credentials to run these integration tests")
public class JMSGetOperationIT {

    private static final String QUEUE_NAME = "QUEUE";
    private static final String ACTUAL_QUEUE_NAME = "integration-tests-jms";

    private final String _providerUrl;
    private String _messageId;
    private String _messagePayload;

    public JMSGetOperationIT() {
        _providerUrl = getClass().getResource("/azure-service-bus.properties").toString();
    }

    /**
     * Remove any message present in the integration test queue for performing a clean test
     */
    @Before
    public void purgeQueue() throws JMSException {
        JMSOperationConnection connection = createConnection(_providerUrl);
        ClientTestUtils.getLastMessage(connection, QUEUE_NAME, Message.class);

        _messagePayload = "payload from Get IT" + UUID.randomUUID();
        Message message = ClientTestUtils.publish(connection, _messagePayload, QUEUE_NAME);
        _messageId = message.getJMSMessageID();
    }

    @Test
    public void getMessageNoWaitTest() {
        JMSOperationConnection connection = createConnection(_providerUrl);
        JMSGetOperation getOperation = new JMSGetOperation(connection);
        SimpleTrackedData document = createInputDocument(QUEUE_NAME);
        SimpleOperationResponse response = ResponseUtil.getResponse(Collections.singleton(document));

        getOperation.executeQuery(ResponseUtil.toRequest(document), response);

        byte[] actualPayload = getPayload(response);
        assertEquals(_messagePayload, new String(actualPayload));
        SimplePayloadMetadata metadata = getMetadata(response);
        assertTrackedProperties(metadata);
    }

    private void assertTrackedProperties(SimplePayloadMetadata metadata) {
        Map<String, String> trackedProps = metadata.getTrackedProps();

        assertEquals("Message ID", _messageId, trackedProps.get("message_id"));
        assertEquals("Destination", ACTUAL_QUEUE_NAME, trackedProps.get("destination"));
        assertEquals("Message Type", "TEXT_MESSAGE", trackedProps.get("message_type"));
        assertEquals("Redelivered", "false", trackedProps.get("redelivered"));
        assertNull("Reply To", trackedProps.get("reply_to"));
        assertNull("Correlation ID", trackedProps.get("correlation_id"));
        assertEquals("Priority", "4", trackedProps.get("priority"));
    }

    private static byte[] getPayload(SimpleOperationResponse response) {
        List<SimpleOperationResult> results = response.getResults();
        assertEquals(1, results.size());
        SimpleOperationResult result = CollectionUtil.getFirst(results);
        assertEquals(OperationStatus.SUCCESS, result.getStatus());
        List<byte[]> payloads = result.getPayloads();
        assertEquals(1, payloads.size());

        SimplePayloadMetadata metadata = CollectionUtil.getFirst(result.getPayloadMetadatas());
        return CollectionUtil.getFirst(payloads);
    }

    private static SimplePayloadMetadata getMetadata(SimpleOperationResponse response) {
        return CollectionUtil.getFirst(CollectionUtil.getFirst(response.getResults()).getPayloadMetadatas());
    }

    private static SimpleTrackedData createInputDocument(String destination) {
        MutableDynamicPropertyMap dynOpProps = new MutableDynamicPropertyMap();
        dynOpProps.addProperty("destination", destination);
        return new SimpleTrackedData(1, null, null, null, dynOpProps);
    }

    private static JMSOperationConnection createConnection(String providerUrl) {
        JMSTestContext context = JMSTestContext.getJMS2GenericWithAzureServiceBusContext(providerUrl);

        context.setOperationType(OperationType.QUERY);
        context.setOperationCustomType("GET");
        context.setObjectTypeId("dynamic_destination");

        context.addOperationProperty("use_transaction", false);
        context.addOperationProperty("use_durable_subscription", false);
        context.addOperationProperty("receive_mode", "NO_WAIT");

        return new JMSOperationConnection(context);
    }
}
